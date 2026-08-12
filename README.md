# Lottery Search System — Design Document

## 1. Problem Restatement

Design (no implementation) a system that lets users search a dataset of lottery tickets using a
6-character pattern of digits and wildcards (`*`), and allocates a matching ticket to a requesting
user such that no two concurrent requests for the same pattern can ever receive the same ticket.

## 2. Key Assumption (and why it matters)

A ticket number is 6 digits, so there are only **1,000,000 possible unique numbers**
(`000000`–`999999`). The dataset has **10,000,000 tickets**, which means the same number must exist
on multiple physical/sellable ticket instances (e.g. ~10 instances per number on average — a batch
print, or tickets re-issued across draws).

This distinction drives the whole design:

- "Search" operates over a **bounded, fixed space of 1,000,000 numbers** — small enough to hold
  entirely in memory.
- "Allocate one ticket" operates over the **10,000,000 instances**, where several rows can share
  the same number and only one of them may be handed to a given caller.

Treating this as one problem (as a naive `LIKE '%23'` scan over 10M rows would) is both slower and
conceptually wrong. Treating it as two layered problems — *which numbers match* (cheap, bounded) and
*which instance of a matching number is still available* (needs atomicity) — is both faster and
simpler to reason about.

## 3. Data Model

```
tickets
  id           BIGINT PRIMARY KEY
  number       CHAR(6) NOT NULL        -- e.g. "123045"
  status       SMALLINT NOT NULL       -- 0=available, 1=reserved, 2=sold
  reserved_by  UUID NULL
  reserved_at  TIMESTAMPTZ NULL

INDEX (number, status)                 -- supports "available instances for number X"
```

No separate table is needed for the number space itself — `000000`–`999999` is generated, not
stored.

## 4. Search & Indexing Strategy

### Layer 1 — Number-level match: in-memory bitmap index

For each of the 6 digit positions and each of the 10 possible digits, keep one bitmap of length
1,000,000 bits (bit `i` = "does number `i` have this digit in this position?"). That's
`6 × 10 = 60` bitmaps, each ~122 KB → **~7.5 MB total**, trivially kept resident in every app
instance's memory (or in Redis, shared).

A separate **availability bitmap** (1,000,000 bits) marks which numbers currently have at least one
`available` instance. It is updated incrementally whenever a ticket's status changes.

To answer a query like `1****5`:

```
result = availability_bitmap
       & position_bitmap[0]['1']
       & position_bitmap[5]['5']
```

Only the fixed positions contribute an AND term (wildcard positions are skipped). This is a handful
of 64-bit word-AND operations over a ~122 KB bitmap — **sub-millisecond**, regardless of pattern
selectivity, and it never touches the 10M-row table.

### Layer 2 — Instance-level allocation

Once a matching, available *number* is identified, a specific *instance* of that number must be
claimed — this is a separate, small, indexed lookup (`WHERE number = ? AND status = 'available'`),
not a full-table concern. See §5.

### Why not a search engine (Elasticsearch/OpenSearch) or SQL wildcard/trigram index?

| Option | Verdict |
|---|---|
| **Bitmap index (chosen)** | Exploits the bounded 1M domain directly; microsecond lookups; ~7.5 MB footprint; no extra infrastructure. |
| SQL `LIKE`/trigram/functional index | Works, but pays per-query index traversal cost proportional to selectivity and doesn't exploit the fact that the domain is small and enumerable. Reasonable fallback, not the best fit. |
| Elasticsearch/OpenSearch wildcard query | Built for unbounded, fuzzy, unstructured text search at large scale. Here the domain is small and structured — adding a search cluster is operational cost without a matching benefit. A tempting "impressive-sounding" answer that doesn't actually fit this problem. |

## 5. Concurrency: Preventing Duplicate Simultaneous Allocation

**Chosen mechanism: PostgreSQL row-level locking via `SELECT ... FOR UPDATE SKIP LOCKED`, inside a
single transaction, per candidate number.**

Allocation flow for a request matching numbers `[n1, n2, ...]` (from §4, Layer 1):

```
for number in candidate_numbers:
    BEGIN
    row = SELECT id FROM tickets
          WHERE number = :number AND status = 'available'
          ORDER BY id LIMIT 1
          FOR UPDATE SKIP LOCKED
    if row exists:
        UPDATE tickets SET status='reserved', reserved_by=:user, reserved_at=now()
               WHERE id = row.id
        COMMIT
        return row.id
    ROLLBACK   -- nothing free/lockable for this number, try the next candidate
```

`FOR UPDATE SKIP LOCKED` means: if another concurrent transaction already holds the lock on a
matching row, this transaction **skips it instead of blocking or ever selecting it** — two
concurrent callers can never walk away with the same instance, and neither has to wait on the
other. This guarantee holds regardless of how many application instances issue the request
concurrently, because the atomicity lives in the database transaction, not in application memory —
important since the API tier is expected to run as multiple stateless replicas (see §7).

A reservation carries a TTL (e.g. 5 minutes) for an in-progress purchase flow. A background sweep
(`UPDATE tickets SET status='available' WHERE status='reserved' AND reserved_at < now() - TTL`)
releases abandoned reservations and flips the number back on in the availability bitmap.

### Why not a distributed lock (Redlock) or an external queue?

Both add moving parts and failure modes (lock expiry vs. work duration, split-brain, an extra
system to operate) to solve a problem the database already solves natively and correctly via
transactional row locks. Reach for them only if profiling shows the database itself is the
bottleneck (see §8) — not by default.

## 6. Storage Technology Choice

**Primary recommendation: PostgreSQL**, as the sole source of truth.

- `FOR UPDATE SKIP LOCKED` gives correct, contention-free concurrent allocation out of the box.
- ACID guarantees matter here — a ticket represents real-world value, so "two users got the same
  ticket" or "a ticket vanished mid-crash" are not acceptable failure modes.
- 10M rows at ~50–100 bytes each is roughly 1 GB of data — small by database standards; the whole
  table and its indexes comfortably fit in memory/OS cache on any modern DB host. No sharding or
  exotic storage is warranted at this scale.
- Mature, boring, well-understood operational story (backup, replication, monitoring) — appropriate
  for a system handling something of monetary value.

**Optional scaling layer: Redis**, added only if load testing shows Postgres write contention on
very hot patterns (e.g. everyone searching `1*****` at once). A Redis `SET` per number holding
available instance IDs, drained with `SPOP` (atomic, O(1)), can absorb that hot-path load, with
Postgres still the durable source of truth (Redis state is rebuildable from Postgres at any time).
This is deliberately **not** part of the baseline design — start with Postgres alone and add this
only when a real bottleneck justifies the added operational complexity (YAGNI).

## 7. Performance Analysis

| Operation | Cost |
|---|---|
| Pattern → candidate numbers (bitmap AND) | A few 64-bit word ops over ~122 KB bitmaps → microseconds, independent of dataset size |
| Claim one instance of a matching number | Single indexed lookup (`number, status`) + row lock → low milliseconds, O(log n) |
| Full dataset footprint | ~1 GB for 10M rows — fits in memory/cache; no partitioning needed at this scale |
| Concurrent throughput | Scales with the number of *distinct* contended numbers, since `SKIP LOCKED` avoids blocking; no global lock or single hot row |

The search step is effectively free at this scale because the real domain is 1M, not 10M — the
allocation step (§5) is where correctness-under-concurrency work actually happens, and it is O(1)
indexed work per attempt.

## 8. Scaling & Production Considerations

- **Stateless API tier**: any number of app replicas can call the same allocation flow safely,
  because correctness is enforced by Postgres transactions, not in-process state.
- **Read scaling**: search is read-heavy; the bitmap index can be recomputed/refreshed on each app
  replica from a periodically-refreshed availability view, or centralized in Redis if replicas need
  to stay tightly in sync. Read replicas can serve search traffic if it ever outgrows a single
  primary's read capacity.
- **Key metric to watch**: `SKIP LOCKED` miss rate (candidate found but already locked/taken) — a
  rising miss rate signals hot-pattern contention and is the concrete trigger for adding the Redis
  allocation layer from §6, rather than adding it speculatively upfront.
- **Reservation TTL sweep** prevents abandoned checkouts from permanently locking tickets away from
  the available pool.

## 9. Summary

- Search exploits a fact the problem statement implies but doesn't state outright: only 1,000,000
  distinct ticket numbers exist, so pattern matching can be solved with a small, in-memory bitmap
  index rather than scanning or indexing 10M rows.
- Duplicate-free allocation is solved with PostgreSQL's native `SELECT ... FOR UPDATE SKIP LOCKED`,
  which gives correct, non-blocking, multi-instance-safe allocation without introducing a
  distributed lock or an extra system.
- The design deliberately starts with the simplest stack that is provably correct (Postgres alone)
  and names the concrete signal (`SKIP LOCKED` miss rate) that would justify adding a Redis
  acceleration layer later, instead of guessing at scale upfront.
