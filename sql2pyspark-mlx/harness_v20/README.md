# harness_v20 — harness-style training pairs (Kimi K3.1 Pro, type-strict Spark check)

Step B of the 2026-11-26 harness plan: more **training** data aimed at what the harness suites
([`sql2pyspark_harness/`](../../sql2pyspark_harness/README.md), [`sql2pyspark_sql_suite/`](../../sql2pyspark_sql_suite/))
show the models getting wrong, without ever training on those suites (they are the test set, `eval/harness_suite_v2.jsonl`).

`generate.py` reuses the [realstyle_v19](../realstyle_v19/README.md) Kimi tool loop with these changes:

- **Targets**: 30 feature types weighted toward the harness gaps — `ARRAY<STRUCT>` generators (`posexplode`,
  `explode_outer`, `inline_outer`), map functions, `from_json` with nested schemas, index-aware higher-order functions,
  `array_sort` with a comparator, `max_by` / `min_by`, `LEFT SEMI` / `LEFT ANTI JOIN`, `NOT IN` vs. `NOT EXISTS` with
  NULLs, `INTERSECT ALL` / `EXCEPT ALL`, `RANGE BETWEEN INTERVAL`, `stack()` / `UNPIVOT`, `PIVOT`, `window()` /
  `session_window()`, date spines with forward fill, gaps-and-islands, SCD2 as-of joins, statistics, exact DECIMAL typing,
  three-valued logic. Each case gets 3-5 of them (2 of weight 3).
- **Size**: 25-80 SQL lines and SQL + code ≤ 8,000 characters, so a pair fits the training sequence length.
- **Check**: `compare_harness.compare_one` (same column names **and** Spark types, row multiset, every value computed)
  plus `verify_pairs.compare` with `order_cols` (the final `ORDER BY`). A Python-side PySpark error reports the failing
  code line.
- **No leakage**: the 13 suite tables and the suite's e-commerce themes are forbidden; 51 domains from realstyle_v19
  minus 3 close to the suite.
- **Hard budget**: a case starts only if spent + the reservations of running cases + one more reservation ($0.94 =
  case cap $0.45 + one worst-case turn) ≤ budget − $0.50; a case stops once it passes the case cap; a turn that dies
  without usage is counted at its worst case. Queued cases wait for room (the first run skipped them instead; fixed).

```bash
cd sql2pyspark-mlx
.venv/bin/python harness_v20/generate.py --n 60 --workers 4 --budget 10 --out harness_v20/pilot
```

## Pilot (2026-11-26 11:52 → 13:36 Bangkok time, $10 cap)

| | |
| --- | --- |
| Cases tried / kept | 44 / **37** (84%) |
| Cost counted | **$8.94** (Kimi-reported tokens $7.11 + $1.84 counted at worst case for turns lost to `APIConnectionError`) |
| Cost per kept pair | $0.19 (tokens) / $0.24 (counted) |
| Median time per case | 285 s (4 in parallel) |
| Kept pairs | median 55 SQL lines, 4,979 chars (SQL + code), 2.8 checks, 28 of the 30 feature types used |
| Failures | 3 lost to a Kimi connection drop at 12:35 (all 4 running cases at once), 1 `Column is not iterable` loop (smoke case, before the code-line hint), 1 row count > 40, 1 schema, 1 data mismatch |

Outputs (git-ignored): `pilot/cases.jsonl` (kept pairs: tables, SQL, code, order_cols, features), `pilot/failed.jsonl`,
`pilot/report.json`, `pilot.log` / `pilot_run1.log`. The pairs are **not** in any training mix yet; a full run
(~2,000 pairs, estimated $300-500) needs the owner's approval.
