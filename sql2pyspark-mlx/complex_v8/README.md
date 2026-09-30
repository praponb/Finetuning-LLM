# complex_v8 — 10,000 complex SQL → PySpark training pairs

A standalone dataset build. Nothing outside this folder was changed; it only *imports* `compare()` / `rows_of()`
from `../verify_pairs.py` and `split()` / `format_sample()` from `../convert_sql_pyspark.py`.

## What is in it

| Path | Content |
|---|---|
| `data/train.jsonl` | **10,000** training pairs (MLX chat format, same system prompt as v1-v7) |
| `data/valid.jsonl`, `data/test.jsonl` | 500 / 300 pairs with held-out feature signatures (+ `*_meta.jsonl` for execution eval) |
| `data/split_stats.json` | counts, token lengths, families, features |
| `data_mix/` | the 10,000 new pairs + 7,000 random v7 training pairs (recommended when continuing from v7) |
| `lora_config.yaml` | ready-to-run config: continues from v7 (`../adapters`), writes `complex_v8/adapters/` |
| `raw_pairs/` | every generated pair (`cx*.sql` / `cx*.py`), `verified.jsonl`, `verify_report.json`, failures |

Every pair was executed in Spark (both sides) and kept only if column names, rows and ORDER BY sequence match.

## How it differs from the v1-v7 data

- **20 new tables in 6 new domains** (music streaming, insurance, warehouse, energy, education, support) —
  none of them appear in earlier training data or in any eval set (`catalog.py`).
- **Complex by construction** — every query is a multi-stage pipeline. Families:

| Family | What it teaches |
|---|---|
| `cx_pipeline` | 3-7 stages: joins (multi / grandparent), EXISTS / NOT EXISTS / IN subqueries, correlated scalar subqueries (→ pre-aggregated LEFT JOIN), dedup with ROW_NUMBER, compare-to-group, LATERAL VIEW [OUTER] [POS]EXPLODE, aggregation with FILTER / CASE / HAVING |
| `cx_window_heavy` | 8-16 window functions over 3-6 specs: named `WINDOW` clauses, ROWS frames, IGNORE NULLS, NTH_VALUE, NTILE, PERCENT_RANK, windows partitioned by date expressions, share-of-total with NULLIF |
| `cx_setop` | UNION ALL / UNION / INTERSECT [ALL] / EXCEPT [ALL] / MINUS, **positional unions of differently named columns (`.union`) vs `.unionByName`**, literal source columns, 3-way unions, union-then-aggregate |
| `cx_sessionize` | gaps-and-islands: LAG → new-session flag (INTERVAL or UNIX_TIMESTAMP gap) → running SUM → per-session metrics |
| `cx_interval` | range joins (coverage periods, tariff windows, open-ticket windows) and time-window self-joins |
| `cx_full_outer` | two aggregates FULL OUTER JOINed with COALESCE keys |
| `cx_grouping` | ROLLUP / CUBE / GROUPING SETS (`.groupingSets`) with GROUPING_ID / GROUPING and 'ALL' labels |
| `cx_reshape` | PIVOT (single / multi aggregate) and UNPIVOT (EXCLUDE / INCLUDE NULLS, column aliases) |

## Rebuild

```bash
cd sql2pyspark-mlx/complex_v8
../.venv/bin/python generate.py --n 13000 --seed 8 --out raw_pairs     # ~1 min
../.venv/bin/python verify.py --src raw_pairs --workers 5 2>/dev/null   # Spark check, ~45-60 min
../.venv/bin/python build.py                                            # split -> data/ + data_mix/
```

## Train

```bash
cd sql2pyspark-mlx          # stop Docker + unload Ollama first (16 GB M2)
.venv/bin/python train_resumable.py --config complex_v8/lora_config.yaml
```

Then export / eval as usual, pointing `mlx_lm.fuse --adapter-path` at `complex_v8/adapters`.
