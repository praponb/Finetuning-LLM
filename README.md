# SQL → PySpark fine-tune

A local, Apple-Silicon pipeline that LoRA-fine-tunes **Qwen2.5-Coder-7B-Instruct (4-bit, MLX; 3B up to v5)** to translate
Spark SQL into idiomatic PySpark DataFrame code, then serves it with **Ollama** behind a small **Streamlit** UI.

> 📖 **Documentation**: [USER_MANUAL.md](USER_MANUAL.md) is the end-user guide (Web UI, CLI, API, SQL support);
> [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) is the single-file technical guide;
> [docs/INDEX.md](docs/INDEX.md) indexes the modular guides (architecture, data, training, evaluation, serving, file catalog, runbook).

**Current model:** `sql2pyspark:latest` = **v9-q4** (since 2026-10-01, promoted by the owner; on the Mac and the
server): v9 as a 4-bit Q4_K_M build (4.7 GB). v9 is v8 continued on the
[complex_v9 dataset](sql2pyspark-mlx/complex_v9/README.md): the complex_v8 queries with **"mirror" window code** (an
inline `Window` spec for `OVER (...)`, a variable only for a SQL-named `WINDOW`) + 1,990 extra many-window pairs.
v9-q4 scores **87/100** on the complex test (v8 60; many-window queries **12/15**, v8 1/15), **140/150** on the fixed
test set (v8 139), **21/25** on the v4 hold-out (v8 18) and 59/62 on the hold-out (v8 60: one `NOT IN` join slip, which
the app's `join_check()` flags), 0/4 on the real-world queries like every model. It is faster too: 39 s vs. 97 s per
complex query on the Mac, and on the CPU-only server 4.05 vs. 2.47 tokens/s. See [v9 results](#v9-results-2026-10-01).
**v8** (the previous `latest`) is kept as `sql2pyspark:v8`: roll back with `ollama cp sql2pyspark:v8 sql2pyspark:latest`
(the server also keeps v7). `sql2pyspark:v9` is the same model at Q8_0 (8.1 GB).

**Candidate:** `sql2pyspark:v10-q4` (trained 2026-10-01, not promoted; the owner decides): v9 continued on the
[classic_v10 dataset](sql2pyspark-mlx/classic_v10/README.md) (the classic pairs in the mirror window style + bare
`IN` / `NOT IN` subqueries). **49/50** on held-out `IN` / `NOT IN` queries (v9-q4 11), **147/150** on the fixed test
(140), **60/62** on the hold-out (59; `holdout024` and `holdout052` fixed), `real001` no longer loops; but **18/25** on the
v4 hold-out (21) and 85/100 on the complex test (87). See [v10 results](#v10-results-2026-10-01).

```
schemas.py ─┐
            ├─ generate_dataset.py ─┐                         ┌─ data/train.jsonl ─ mlx_lm.lora ─ adapters/
handwritten_pairs.py ───────────────┴─ raw_pairs/ ─ verify ─ split ┤                        │
                                         (Spark: SQL result ==     └─ valid/test.jsonl     mlx_lm.fuse (--dequantize)
                                          PySpark result)                                  │
                                                                   sql2pyspark.gguf ◄─ llama.cpp convert_hf_to_gguf (Q8_0)
                                                                          │
                                                        ollama create sql2pyspark:<tag> ─ app.py (Streamlit)
                                                                          │
                                                          eval/evaluate.py (execution-based scoring)
```

## Setup

```bash
brew install openjdk@17          # PySpark needs Java
make setup                       # venv in sql2pyspark-mlx/.venv + requirements.txt
```

Ollama must be installed and running (`open -a Ollama`). The app and the evaluator talk to
`http://localhost:11434` by default; set `OLLAMA_URL` to use another Ollama server.

Editor type-checking (Pyrefly) is configured in [`pyrefly.toml`](pyrefly.toml) to use the project venv.

## Pipeline

| Step | Command | What it does |
|---|---|---|
| Data | `make data N=20000 SEED=5` | Generate 20k pairs + 105 hand-written pairs, execute both sides of every pair in local Spark, keep only pairs whose results match, split by feature signature (~20 min). |
| Train | `make train` | `train_resumable.py` runs `mlx_lm.lora -c sql2pyspark-mlx/lora_config.yaml` and resumes from the last checkpoint if Metal aborts a step (v6: Qwen2.5-Coder-7B-Instruct-4bit, batch 1 × 4 accumulation, ~8.5 h; v5: continued from v4's 3B adapters with peak LR 5e-5; ~5 h 40 min on a base M2). |
| Export | `make export TAG=v9` | Fuse adapters, dequantize, convert to Q8_0 GGUF, `ollama create sql2pyspark:v9`. |
| Evaluate | `make eval TAG=v9 PREV=v8` | Score both models on the hold-out set, the v4 hold-out set, the real-world queries and the same sample of the test split. |
| Promote | `make promote TAG=v8 PREV=v7` | Point `sql2pyspark:latest` (what `app.py` uses by default) at the new model; keeps the old one as `:$(PREV)`. |
| UI | `make app` | Streamlit converter on http://localhost:8501; the sidebar field picks any Ollama tag. |

`make help` lists every target. The Makefile defaults are `TAG=v9` and `PREV=v8` (v8 exists, so promote it explicitly with `TAG=v8 PREV=v7`) (`BASE` is `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` since v6; v1–v5 used 3B). On a 16 GB Mac,
don't run Ollama (evaluations, the app) or Docker while training: Ollama plus training ran out of GPU memory once, and
Docker Desktop's VM takes another 8 GB.

## Dataset (v5)

* **`sql2pyspark-mlx/schemas.py`**: catalog of 17 base training tables (+ 5 extra tables for v4: `carts`, `app_logs`,
  `price_tiers` with nested data, `device_readings` and `promotions` with dense time series / validity windows) and 5
  base hold-out tables (+ the nested hold-out table `vehicle_trips`), with typed columns, foreign keys, realistic value
  pools, nested types (structs, arrays of structs, maps, JSON strings) and seeded sample data for execution checks.
* **`sql2pyspark-mlx/generate_dataset.py`** + **`sqlspec.py`**: compositional generator. Each sample picks a family
  (select, aggregation, joins, join+aggregation, windows, top-N per group, CTE + join, CTE chains, derived tables,
  IN/EXISTS/NOT EXISTS/scalar/correlated subqueries, set operations, pivot/explode/cross join, window-over-aggregate)
  and fills it with random building blocks: boolean filter trees, string/date/numeric functions, CASE, CAST,
  COALESCE, conditional aggregates, frames, HAVING, ORDER BY/LIMIT, … SQL surface style varies (case, layout,
  aliases). SQL and PySpark are rendered from the same objects, so they agree by construction.
* **`sql2pyspark-mlx/advanced.py`** (v4): stage-based families. A query is a chain of stages, each one a CTE: cleaning
  with timestamp literals, ROW_NUMBER/RANK dedup (QUALIFY or subquery), 2-5 different windows (LAG/LEAD, running and
  `ROWS n PRECEDING` frames, epoch `RANGE` frames, `NULLS FIRST/LAST`, CUME_DIST, NTILE, `LAST_VALUE ... IGNORE NULLS`,
  named `WINDOW` clauses), gaps-and-islands sessions with `INTERVAL`, joins (foreign key, `INTERVAL` / `BETWEEN` validity
  windows, `<=>`, `COALESCE` keys), `LATERAL ... LIMIT 1`, `[NOT] EXISTS` with extra correlated conditions,
  `EXCEPT/INTERSECT [ALL|DISTINCT]`, aggregation (`GROUPING SETS`, `ROLLUP`, `CUBE`, `GROUPING_ID`, `FILTER (WHERE ...)`,
  `COUNT_IF`, `NULLIF` ratios, `PERCENTILE_APPROX`, `CORR`, `BIT_OR`, sorted `COLLECT_SET`), multi-aggregate `PIVOT`,
  `UNPIVOT`, final reports (correlated scalar subqueries in SELECT, `RLIKE`), and nested data (`LATERAL VIEW [OUTER]
  EXPLODE/POSEXPLODE`, `INLINE`, struct fields, map lookups, `TRY_CAST`, `FROM_JSON`, `GET_JSON_OBJECT`, `TRANSFORM`,
  `FILTER`, `EXISTS`, `FORALL`, `AGGREGATE`, `ZIP_WITH`, bit masks). One-stage chains are short feature queries;
  `fam_pipeline` chains 4-8 stages into production-style queries of up to ~2.7k tokens.
* **`sql2pyspark-mlx/verify_pairs.py`**: runs `spark.sql(sql)` and the PySpark code on the sample tables and
  compares column names, rows and ORDER BY sequence. Only passing pairs are used (v5: 20,073 of 20,105; v4: 20,068 of 20,105; v3: all 15,105).
* **`sql2pyspark-mlx/convert_sql_pyspark.py`**: chat-format JSONL. Whole feature signatures are held out for
  valid/test, so validation loss measures generalisation to unseen feature combinations.

v4 dataset: 20,068 verified pairs (train 16,924 / valid 2,096 / test 1,048); 42% come from the v4 families and 16%
are long pipelines (median ~1,100 tokens, max 3,119; the v3 data maxed at 603). `UNION` (with `.distinct()`) is now
drawn more often than `UNION ALL`.

v3 changes to the generator, aimed at the v2 hold-out failures: set operations over differently named columns
(`union()` / aliases), `GROUP BY` ordinals on `CASE` keys, and exact `NOT IN` semantics
(`isNotNull() | F.lit(excluded.isEmpty())`). The v2 and v3 adapters and logs are kept in
`sql2pyspark-mlx/runs/v2/` and `runs/v3/` (their datasets were removed on 2026-09-30 and are still in the git history).

### Output conventions (what the model learns to write)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window        # only when needed

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees                              # inputs are always df_<table>
    .filter(F.col("is_remote"))
    .select("id", "dept_id", "salary", F.rank().over(w).alias("salary_rank"))
)                                             # final DataFrame is always `result`
```

* the final column list, order and aliases match the SQL select list;
* joins use `df_x.alias("a")` + `F.col("a.col")`, mirroring SQL aliases;
* aggregates always use `.agg(F.count("*").alias(...))`, never a bare `.count()`;
* `UNION ALL` → `unionByName` (or `union` when the two sides' column names differ, since SQL matches by position),
  `UNION` → the same plus `.distinct()`, `EXCEPT` → `subtract`;
* CTEs become named intermediate DataFrames; scalar subqueries become a value computed up front.

## Evaluation

`eval/build_holdout.py` → `eval/holdout.jsonl`: 62 hand-written queries (15 simple, 22 medium, 25 complex) over the
hold-out tables, each with a verified reference answer. `eval/evaluate.py` sends each query to an Ollama model with
the same prompt as `app.py`, executes the returned code and compares it with the SQL result.

`eval/build_holdout_v4.py` → `eval/holdout_v4.jsonl`: 25 hand-written queries for the v4 features (aggregation,
window, join, set operation, reshape, nested and pipeline groups) on the hold-out tables, including the nested
`vehicle_trips`.

`eval/build_real_queries.py` → `eval/real_queries.jsonl`: 4 real-world queries supplied by users, run against the
hand-made tables in `eval/real_tables.py` (scored with `--tables eval/real_tables.py`):
1. `real001` (`marketing_attribution`): 4-CTE report with sessionization, interval joins, dedup windowing, grouping sets.
2. `real002` (`order_fulfillment`): higher-order array lambdas, lateral view explode, RANGE windows, QUALIFY, PIVOT, correlated SLA subquery.
3. `real003` (`iot_telemetry`): complex JSON parsing, bitwise masks, native UNPIVOT, correlated LATERAL join, stateful imputation with `LAST_VALUE IGNORE NULLS`, CUME_DIST, MAP_FROM_ARRAYS, aggregate FILTER.
4. `real004` (`trade_analytics`): regex tokens, `TRY_CAST`, higher-order `ZIP_WITH` and `AGGREGATE` (fold/reduce), sessionization gaps-and-islands, named windows, CUBE with `GROUPING_ID`, `APPROX_PERCENTILE`.

* **strict**: code must run as-is and assign `result` (the convention since v2).
* **lenient**: imports added, bare `df` bound to the main table, last expression taken as the result (fair to v1).

The table below is generated by `python eval/report.py --readme README.md`. The `test_fixed` rows use the fixed test set
`eval/test_fixed.jsonl` (150 queries of the v4 test split, kept out of v5 training); results on older splits are in
`eval/results/v3-split/` (v2 95%, v3 95%) and `eval/results/v2-split/` (v1 8%, v2 98%). `real003` and `real004` carry a `ref_sql`: Spark rejects string-literal
`UNPIVOT` aliases and a repeated `GROUPING_ID(... CASE ...)` in `HAVING`, so the expected result comes from a rewrite
with the same meaning while the model gets the original SQL.
Replies are capped at 2,048 tokens (`--num-predict`; 1,024 before 2026-09-27, which only ever cut off the real query).

<!-- RESULTS:START -->
| Model | Eval set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
|---|---|---|---|---|---|---|---|---|
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v10-q4` | holdout | 62 | 98% | 97% | **97%** | 97% | **97%** | 4.9 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v4` | holdout | 62 | 100% | 95% | **94%** | 95% | **94%** | 4.0 |
| `sql2pyspark:v5` | holdout | 62 | 97% | 92% | **90%** | 92% | **90%** | 4.3 |
| `sql2pyspark:v6` | holdout | 62 | 100% | 97% | **97%** | 97% | **97%** | 9.9 |
| `sql2pyspark:v7` | holdout | 62 | 100% | 98% | **98%** | 98% | **98%** | 9.1 |
| `sql2pyspark:v8` | holdout | 62 | 100% | 97% | **97%** | 97% | **97%** | 9.3 |
| `sql2pyspark:v9` | holdout | 62 | 100% | 95% | **95%** | 95% | **95%** | 7.3 |
| `sql2pyspark:v9-q4` | holdout | 62 | 100% | 95% | **95%** | 95% | **95%** | 8.3 |
| `sql2pyspark:v10-q4` | holdout_v4 | 25 | 100% | 80% | **72%** | 80% | **72%** | 11.5 |
| `sql2pyspark:v2` | holdout_v4 | 25 | 92% | 32% | **20%** | 32% | **20%** | 6.5 |
| `sql2pyspark:v3` | holdout_v4 | 25 | 88% | 32% | **12%** | 32% | **12%** | 7.1 |
| `sql2pyspark:v4` | holdout_v4 | 25 | 96% | 60% | **36%** | 60% | **36%** | 7.0 |
| `sql2pyspark:v5` | holdout_v4 | 25 | 88% | 68% | **56%** | 68% | **56%** | 8.1 |
| `sql2pyspark:v6` | holdout_v4 | 25 | 100% | 80% | **64%** | 80% | **64%** | 18.4 |
| `sql2pyspark:v7` | holdout_v4 | 25 | 100% | 80% | **64%** | 80% | **64%** | 16.4 |
| `sql2pyspark:v8` | holdout_v4 | 25 | 100% | 76% | **72%** | 76% | **72%** | 17.1 |
| `sql2pyspark:v9` | holdout_v4 | 25 | 100% | 76% | **72%** | 76% | **72%** | 13.3 |
| `sql2pyspark:v9-q4` | holdout_v4 | 25 | 100% | 84% | **84%** | 84% | **84%** | 16.2 |
| `sql2pyspark:v10-q4` | real_queries | 4 | 100% | 25% | **0%** | 25% | **0%** | 144.4 |
| `sql2pyspark:v2` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 55.3 |
| `sql2pyspark:v3` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 78.9 |
| `sql2pyspark:v4` | real_queries | 4 | 25% | 0% | **0%** | 0% | **0%** | 92.8 |
| `sql2pyspark:v5` | real_queries | 4 | 50% | 0% | **0%** | 0% | **0%** | 75.6 |
| `sql2pyspark:v6` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 189.4 |
| `sql2pyspark:v7` | real_queries | 4 | 50% | 0% | **0%** | 0% | **0%** | 163.6 |
| `sql2pyspark:v8` | real_queries | 4 | 100% | 0% | **0%** | 0% | **0%** | 195.5 |
| `sql2pyspark:v9` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 131.2 |
| `sql2pyspark:v9-q4` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 164.5 |
| `sql2pyspark:v10-q4` | test_fixed | 150 | 100% | 99% | **98%** | 99% | **98%** | 17.1 |
| `sql2pyspark:v3` | test_fixed | 150 | 92% | 73% | **69%** | 73% | **70%** | 8.0 |
| `sql2pyspark:v4` | test_fixed | 150 | 90% | 80% | **73%** | 80% | **73%** | 11.6 |
| `sql2pyspark:v5` | test_fixed | 150 | 97% | 88% | **85%** | 89% | **85%** | 11.2 |
| `sql2pyspark:v6` | test_fixed | 150 | 99% | 91% | **88%** | 91% | **88%** | 22.5 |
| `sql2pyspark:v7` | test_fixed | 150 | 100% | 93% | **91%** | 93% | **91%** | 25.8 |
| `sql2pyspark:v8` | test_fixed | 150 | 99% | 96% | **93%** | 96% | **93%** | 24.3 |
| `sql2pyspark:v9` | test_fixed | 150 | 99% | 94% | **89%** | 94% | **89%** | 19.4 |
| `sql2pyspark:v9-q4` | test_fixed | 150 | 100% | 97% | **93%** | 97% | **93%** | 18.6 |
| `sql2pyspark:v10-q4` | complex_v8_test | 100 | 98% | 90% | **85%** | 90% | **85%** | 53.6 |
| `sql2pyspark:v7` | complex_v8_test | 100 | 83% | 38% | **31%** | 38% | **31%** | 72.9 |
| `sql2pyspark:v8` | complex_v8_test | 100 | 96% | 68% | **60%** | 68% | **60%** | 96.9 |
| `sql2pyspark:v9` | complex_v8_test | 100 | 100% | 92% | **87%** | 92% | **87%** | 57.9 |
| `sql2pyspark:v9-q4` | complex_v8_test | 100 | 99% | 91% | **87%** | 91% | **87%** | 39.2 |

Hold-out, correct results (lenient) by difficulty:

| Model | simple (n=15) | medium (n=22) | complex (n=25) |
|---|---|---|---|
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v10-q4` | 100% | 95% | 96% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | 100% | 95% | 92% |
| `sql2pyspark:v4` | 100% | 95% | 88% |
| `sql2pyspark:v5` | 93% | 95% | 84% |
| `sql2pyspark:v6` | 93% | 95% | 100% |
| `sql2pyspark:v7` | 100% | 95% | 100% |
| `sql2pyspark:v8` | 100% | 91% | 100% |
| `sql2pyspark:v9` | 100% | 91% | 96% |
| `sql2pyspark:v9-q4` | 100% | 91% | 96% |

v4 hold-out, correct results (lenient) by feature group:

| Model | aggregation (n=4) | window (n=5) | join (n=5) | setop (n=3) | reshape (n=2) | nested (n=4) | pipeline (n=2) |
|---|---|---|---|---|---|---|---|
| `sql2pyspark:v10-q4` | 75% | 100% | 80% | 33% | 0% | 75% | 100% |
| `sql2pyspark:v2` | 0% | 40% | 20% | 67% | 0% | 0% | 0% |
| `sql2pyspark:v3` | 0% | 40% | 0% | 33% | 0% | 0% | 0% |
| `sql2pyspark:v4` | 25% | 40% | 20% | 67% | 50% | 50% | 0% |
| `sql2pyspark:v5` | 50% | 100% | 40% | 67% | 50% | 50% | 0% |
| `sql2pyspark:v6` | 75% | 80% | 60% | 67% | 50% | 50% | 50% |
| `sql2pyspark:v7` | 50% | 80% | 60% | 33% | 100% | 75% | 50% |
| `sql2pyspark:v8` | 100% | 60% | 60% | 67% | 100% | 75% | 50% |
| `sql2pyspark:v9` | 75% | 100% | 100% | 33% | 50% | 50% | 50% |
| `sql2pyspark:v9-q4` | 75% | 100% | 100% | 67% | 50% | 75% | 100% |
<!-- RESULTS:END -->

**v3 vs. v2:** v3 fixes 3 of v2's 6 hold-out failures (`GROUP BY 1` on a `CASE`, two unbalanced-parenthesis slips)
with no regressions. On the test sample the two are tied (v2 286/300, v3 284/300).

### v10 results (2026-10-01)

| Eval set | **v9-q4** (`latest`) | **v10-q4** | Rule (vs. v9-q4) |
|---|---|---|---|
| Hold-out (62, unseen tables) | 59 | **60** | ✅ needs ≥ 59 (`holdout024`, `holdout052` fixed; `holdout041` lost: one extra `)`) |
| Fixed test set: classic families (97) | 97 | **97** | ✅ |
| Fixed test set: v4 families (53, incl. 22 pipelines) | 43 (14) | **50 (19)** | — |
| Fixed test set: all (150) | 140 | **147** | ✅ needs ≥ 140 |
| v4 hold-out (25, new features) | 21 | **18** | ❌ needs ≥ 21 |
| Real-world queries (4) | 0 | 0 | ✅ `real001` no longer loops (no window variables; fails on `F.distinct`) |
| complex_v8 test (100 of 300) | 87 | **85** | ❌ needs ≥ 87 (5 gained, 7 lost) |
| Held-out `IN` / `NOT IN` pairs (50, `classic_v10/data/inlist_valid_meta.jsonl`) | 11 | **49** | ✅ |
| Seconds per query: hold-out / fixed test / complex test | 8.3 / 18.6 / 39.2 | 4.9 / 17.1 / 53.6 | — |

**Why v10**: two failure causes v9 left in the classic data. (1) The classic v1–v7 targets still hoisted every window
spec (`w`, `w2`, … `w21`), and `real001` (classic tables) looped on exactly that block (`w70`, `w71`, …).
`classic_v10/mirror.py` rewrote the 7,858 classic pairs with windows to the mirror style (7,857 still match in Spark).
(2) Every classic `IN` subquery aliased the outer table in the SQL, so on the bare form (`WHERE id NOT IN (SELECT
doctor_id FROM appointments)`) v8/v9 wrote an unqualified `F.col("id")` in the join condition (ambiguous).
`classic_v10/inlist.py` added 900 such pairs (900/900 verified; 50 held out).

**Training**: v9's adapters continued on `classic_v10/data_mix` (6,646 mirrored classic + 817 `IN` / `NOT IN` + 2,000
classic without windows + 3,000 complex_v9 = 12,463), 2,400 micro-batches of 1 (600 updates of 4, peak LR 5e-5),
2026-10-01 12:18 → 18:42 (6 h 24 min, peak 9.9 GB, no crash); validation loss 0.013 → 0.012 → 0.006 → **0.007**.

* **`IN` / `NOT IN` 49/50** (v9-q4 11/50: 34 of its 39 misses are the ambiguous `id`, 4 more are other join-column errors). The one miss (`inl00420`)
  returns 0 rows instead of 5.
* **Fixed test 147/150**: pipelines 19/22 (v9-q4 14), advanced families 31/31.
* **Hold-out 60/62**: both `IN` / `NOT IN` slips are fixed; new miss `holdout041` (an extra `)` after `.over(...)`).
* **`real001`** now writes its windows inline and ends normally (148 s); it fails on a made-up `F.distinct`, so the real
  queries stay 0/4 (they are long, and Kimi K3 repair is still needed there).
* **v4 hold-out 18/25** (−3 net: 4 lost, 1 gained). One loss is systematic and comes from the new data: on a correlated
  `NOT EXISTS` with an outer column in a non-equality condition (`a.appt_date >= p.birth_date`, `holdout_v4_012`) v10
  puts that condition in the inner `.filter()`, where `p` does not exist, instead of in the join condition. The others:
  `INTERSECT ALL` before the `select` (017), renamed pivot columns (018), a filter before `posexplode` (021).
* **complex test 85/100** (−2: 5 gained, 7 lost across families), within the noise of a 100-query sample. The answers are
  as long as v9-q4's (2,152 vs. 2,126 characters); the higher time per query was not seen on the other sets (hold-out
  4.9 vs. 8.3 s), so it is likely load on the Mac rather than the model.

**Recommendation**: by the rule v10-q4 does not pass (v4 hold-out −3, complex −2). Without the targeted `IN` / `NOT IN`
set the two models are even (310 vs. 307 correct over hold-out, v4 hold-out, fixed test and complex test), and v10-q4
fixes two real failure modes (bare `IN` / `NOT IN`, the `real001` loop) that the app otherwise needs Kimi for. Promoting
it is the owner's call: `make promote TAG=v10-q4 PREV=v9-q4`, then
`GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v10-q4 v9-q4`. Next data target: correlated `EXISTS` / `NOT EXISTS`
with outer columns in non-equality conditions (keep them in the join condition).

### v9 results (2026-10-01)

| Eval set | v8 | v9 (Q8_0) | **v9-q4 (Q4_K_M)** | Rule (vs. v8) |
|---|---|---|---|---|
| Hold-out (62, unseen tables) | 60 | 59 | **59** | ❌ needs ≥ 60 (both: new miss `holdout052`) |
| Fixed test set: classic families (97) | 97 | 96 | **97** | v9 ❌, v9-q4 ✅ |
| Fixed test set: all (150) | 139 | 134 | **140** | v9 ❌, v9-q4 ✅ needs ≥ 139 |
| v4 hold-out (25, new features) | 18 | 18 | **21** | v9 tie, v9-q4 ✅ |
| Real-world queries (4) | 0 | 0 | 0 | ❌ (as every model) |
| complex_v8 test (100 of 300) | 60 | 87 | **87** | ✅ |
| … many-window queries (`cx_window_heavy`, 15) | 1 | 11 | **12** | — |
| … pipelines (`cx_pipeline`, 32) | 15 | 26 | **27** | — |
| Seconds per query: hold-out / fixed test / complex test | 9.3 / 24.3 / 96.9 | 7.3 / 19.4 / 57.9 | 8.3 / 18.6 / 39.2 | — |

**Why v9**: v8 scored 1/15 on the many-window family although it had 1,690 training pairs of it. Its answers showed the
cause: the v8 target code defined *every* window spec as a variable at the top of the script, so the model had to plan
8–16 near-identical lines first, and it repeated one (`w_..._56`) until the token limit, used names it never defined, or
attached the wrong frame. complex_v9 keeps the SQL, tables and split of complex_v8 (so the complex test stays held out)
and only changes the PySpark side to **mirror** the SQL: `OVER (...)` → an inline `Window...` spec, `OVER w` + `WINDOW w
AS (...)` → a variable `w` defined right before the stage that uses it (`complex_v8/generate.py --window-style mirror`;
the default `hoist` reproduces complex_v8 byte for byte). 15,499 of the 15,500 re-rendered pairs match in Spark.

**Training**: v8's adapters continued on `complex_v9/data_mix` (the 7,251 pairs with windows + 2,000 complex pairs
without + 4,000 v7 pairs = 13,251), 2,400 micro-batches of 1 (600 updates of 4, peak LR 5e-5), 2026-09-30 19:05 →
10-01 05:23 (10 h 18 min, peak 10.0 GB, no crash); validation loss 0.034 → 0.018 → 0.007 → **0.008**.

* **complex_v8 test 87/100** (v8 60): many-window queries 11/15 (v8 1/15), pipelines 26/32 (15), reshape 8/8, grouping
  5/5, sessions 12/12; 40% less time per query because the answers no longer loop.
* **Hold-out 59/62**: the two old misses (`holdout024`, `holdout031`) plus `holdout052`: `NOT IN (subquery)` becomes a
  `left_anti` join with an unqualified `F.col("id")` (ambiguous), the same slip as `holdout024`. The app's new
  `join_check()` flags both, so the Kimi K3 repair runs on them.
* **Real-world queries 0/4**: `real001` still loops (`w70`, `w71`, … all the same spec). Those queries use the classic
  tables, and the 4,000 v7 pairs in the mix still hoist their windows, so the mirror style did not carry over there.
* **v9-q4 vs. v9**: the Q4_K_M build (made with `llama-quantize` from the same f16 export) is equal or better on every
  set; on sets this small (+3 on the v4 hold-out, +6 on the fixed test) that is noise, not a gain from quantizing, but it
  shows the 4-bit build costs nothing measurable here. It is also faster on the Mac (39 vs. 58 s per complex query).
  On the CPU-only server it generates 1.64× as fast as v8 (4.05 vs. 2.47 tokens/s).

**Decision**: v9 misses the rule on the hold-out and the fixed test; **v9-q4 misses only the hold-out, by one query**
(`holdout052`, caught by `join_check`). The owner **promoted v9-q4** on 2026-10-01 (`make promote TAG=v9-q4 PREV=v8`)
and put it on the server (`GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8`); on the server's CPU v9-q4 generates **4.05 tokens/s vs. v8's 2.47** (1.64×): the app's default GROUP BY query takes **23.5 s vs. 38.5 s** warm, and loading the model takes 16 s vs. 36 s (measured 2026-10-01).
Next data targets: the mirror style for the classic generators (`real001`), and qualified columns in `IN` / `NOT IN`
semi/anti joins.

### v8 results (2026-09-30)

| Eval set | v6 | v7 | **v8** | Rule for v8 (vs. v7) |
|---|---|---|---|---|
| Hold-out (62, unseen tables) | 60 | 61 | **60** | ❌ needs ≥ 61 |
| Fixed test set: classic families (97) | 96 | 97 | **97** | ✅ needs ≥ 94 |
| Fixed test set: v4 families (53, incl. 22 pipelines) | 36 (10) | 40 (12) | **42 (13)** | — |
| Fixed test set: all (150) | 132 | 137 | **139** | ✅ needs ≥ 137 |
| v4 hold-out (25, new features) | 16 | 16 | **18** | ✅ needs > 16 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ needs ≥ 1 |
| Real-world queries + Kimi K3 repair (sample / empty tables), 1 run | 3 / 4 | 4 / 3 | **4 / 4** | — |
| complex_v8 test (100 of 300, new) | — | 31 | **60** | — |
| Seconds per hold-out query | 9.9 | 9.1 | 9.3 | — |

v8 continues from v7's adapters on `complex_v8/data_mix`: the 10,000 new pairs of the standalone
[complex_v8](sql2pyspark-mlx/complex_v8/README.md) build (20 new tables in 6 domains; multi-stage pipelines, 8–16-window
queries, positional set operations, grouping sets, PIVOT/UNPIVOT, sessions, range joins, FULL OUTER JOINs; 12,999 of
13,000 generated pairs match in Spark) plus 7,000 v7 pairs. 800 updates (3,200 micro-batches of 1, peak LR 5e-5;
11 h 42 min because the samples are ~2× longer; validation loss 0.135 → 0.021 at step 800 → 0.016 → 0.012 → **0.011**).

* **complex_v8 test 60/100** (v7 31): set operations 21/25 (v7 4), pipelines 15/32 (8), sessions 11/12, grouping sets
  4/5, range joins 3/3, PIVOT/UNPIVOT 5/8 (v7 6), but **many-window queries only 1/15** (v7 0). Same generator and tables
  as the training data (unseen feature combinations only), so treat this gain as optimistic.
* **Hold-out 60/62**: new miss `holdout024`, a simple `IN (subquery)` where v8 writes `F.col("id") == F.col("patient_id")`
  in a `left_semi` join without aliases (ambiguous `id`); `holdout031` (positional `UNION` of `city` and `hospital`)
  still uses `.unionByName()`, even though v8 learned positional unions on the new tables.
* **v4 hold-out 18/25**: gains 5 (`002`, `004`, `014`, `017`, `021`), loses 3 (`006`, `011`, `023`).
* **Fixed test set 139/150**: 6 gains (4 pipelines), 4 losses (3 pipelines, 1 window); all 97 classic queries correct.
* **Real-world queries 0/4**: all 4 replies now parse (v7: 2) but none runs (`real001` never assigns `result`, an
  undefined alias, `WindowSpec.rows`, `F.try_cast`). The Kimi K3 repair fixes **4/4 on sample tables ($0.91) and 4/4 on
  empty tables ($0.81)** (`eval/results/v8-repair/`).

**Decision**: it misses the hold-out condition by one query (and, like every model, the real-world one) but gains on
every other set; the owner **promoted** it on 2026-09-30 (`make promote TAG=v8 PREV=v7`) and put it on the Ubuntu server
(`deploy/update_model.sh v8 v7`). Roll back with `ollama cp sql2pyspark:v7 sql2pyspark:latest`.

### v7 results (2026-09-29)

| Eval set | v5 | v6 | **v7** | Rule for v7 (vs. v6) |
|---|---|---|---|---|
| Hold-out (62, unseen tables) | 56 | 60 | **61** | ✅ needs ≥ 60 |
| Fixed test set: classic families (97) | 95 | 96 | **97** | ✅ needs ≥ 94 |
| Fixed test set: v4 families (53, incl. 22 pipelines) | 33 (8) | 36 (10) | **40 (12)** | — |
| Fixed test set: all (150) | 128 | 132 | **137** | ✅ needs ≥ 132 |
| v4 hold-out (25, new features) | 14 | 16 | 16 | ❌ needs > 16 (tie) |
| Real-world queries (4) | 0 | 0 | 0 | ❌ needs ≥ 1 |
| Real-world queries + Kimi K3 repair (sample / empty tables), 1 run | 3 / 3 | 3 / **4** | **4** / 3 | — |
| … with the Spark-default rules, 3 runs (of 12) | — | — | 11 / **12** | — |
| Seconds per hold-out query | 4.3 | 9.9 | 9.1 | — |

v7 continues from v6's adapters for 600 updates (2,400 micro-batches of 1, peak LR 5e-5; 4 h 58 min, validation loss
0.011 → 0.021 at step 800 → 0.010 → **0.009**) on a new dataset (`SEED=7`, 20,066 of 20,105 pairs verified) whose
generator adds three patterns aimed at v6's failures: `DATEDIFF(...)` arithmetic (`/ 365`, `/ 7`; 321 pairs), `UNION`
of unrelated columns with `IS NOT NULL` filters (144 + 133 pairs) and windows partitioned by a date expression
(`PARTITION BY DATE_TRUNC('month', ts)`, `YEAR(d)`; 1,578 pairs; none existed before).

* **Hold-out 61/62**: `holdout015` (`DATEDIFF(...) / 365`) is fixed; since the new data targeted it, count this gain
  as optimistic. `holdout031` still uses `.unionByName()` for a positional `UNION` of `city` and `hospital`.
* **Fixed test set 137/150** (not targeted): 7 queries right that v6 misses (3 pipelines, 3 aggregations, a nested
  query) and 2 wrong that v6 gets right; all 97 classic queries correct.
* **v4 hold-out 16/25**: gains 2 (`holdout_v4_019`, `holdout_v4_023`) and loses 2 (`holdout_v4_002`, `holdout_v4_017`).
* **Real-world queries 0/4**: `real001` still loops (88 window definitions) despite the expression-partition data; only
  2 of 4 replies parse (v6: 3). With the Kimi K3 repair and the added rules for `UNPIVOT` NULL exclusion and positional `UNION`
  handling, v7's answers were repaired 11/12 times on sample tables and 12/12 on empty tables over 3 runs per mode
  (`eval/results/v7-repair-rules/`; before the rules one run per mode gave 4/4 and 3/4).

**Decision**: v7 beats or ties v6 on every set and misses two rule conditions: the v4 hold-out is a tie and no model
has solved a real-world query yet (v6 was promoted with the latter). The owner promoted it on 2026-09-29
(`make promote TAG=v7 PREV=v6`); roll back with `ollama cp sql2pyspark:v6 sql2pyspark:latest`.

### v6 results (2026-09-28)

| Eval set | v3 | v5 | **v6 (7B)** | Rule for v6 |
|---|---|---|---|---|
| Hold-out (62, unseen tables) | 59 | 56 | **60** | ✅ needs ≥ 56 (v5) |
| Fixed test set: classic families (97) | 96 | 95 | **96** | ✅ needs ≥ 94 |
| Fixed test set: v4 families (53, incl. 22 pipelines) | 9 (0) | 33 (8) | **36 (10)** | — |
| Fixed test set: all (150) | 105 | 128 | **132** | ✅ needs ≥ 128 |
| v4 hold-out (25, new features) | 3 | 14 | **16** | ✅ needs > 14 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ needs ≥ 1 |
| Seconds per hold-out query | 3.3 | 4.3 | 9.9 | — |

v6 is the first model on `Qwen2.5-Coder-7B-Instruct-4bit`: fresh LoRA adapters (rank 16, last 8 of 28 blocks) trained
for 1,200 updates of 4 samples on the v5 dataset, as 4,800 micro-batches of 1 (batch 2 runs out of GPU memory with the
7B). Training took 8 h 19 min on a base M2 (peak 10.1 GB, no crashes), validation loss 0.289 → **0.010** (v5: 0.012).
It fixes 4 of v5's 6 hold-out failures (incl. the long-standing `UNION ALL` alias case `holdout053` and the
`CROSS JOIN` of two `SELECT DISTINCT`s `holdout058`) with no new ones; on the fixed test set it gets 7 queries right
that v5 misses (3 pipelines) and misses 3 that v5 gets right. It was promoted: it beats or ties v5 on every set, and
the one failed condition, a real-world query, has never been met by any model (v5 was promoted by override while
missing it too).

* **Hold-out failures**: `holdout015` (`DATEDIFF(CURRENT_DATE(), birth_date)` written as `F.current_date() - "birth_date"`,
  which subtracts the string literal, plus a misplaced `.alias`) and `holdout031`
  (`UNION` of differently named columns, failing since v3).
* **Real-world queries 0/4**: `real001` still repeats window definitions (`w1` … `w92`) until the reply is cut off,
  also with the app's `repeat_last_n=2048` (which stopped v5's loop), so the app relies on its check + Kimi repair there;
  `real002`-`real004` now parse (v5: 2 of 4) but fail on a column qualifier or a `grouping_id` column.

### v5 results (2026-09-28)

| Eval set | v3 | v4 | v5 | Rule for v5 |
|---|---|---|---|---|
| Hold-out (62, unseen tables) | 59 | 58 | 56 | ❌ needs ≥ 59 |
| Fixed test set: classic families (97) | 96 | 84 | **95** | ✅ needs ≥ 94 |
| Fixed test set: v4 families (53, incl. 22 pipelines) | 9 (0) | 26 (5) | **33 (8)** | — |
| Fixed test set: all (150) | 105 | 110 | **128** | ✅ needs ≥ 105 |
| v4 hold-out (25, new features) | 3 | 9 | **14** | ✅ needs > 3 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ needs ≥ 1 |

v5 continued from v4 for 1,200 more updates (peak LR 5e-5) on a 55% classic / 25% short-feature / 20% pipeline mix
with the v5 generator fixes (exact projections, `<=>`, `GROUPING SETS` over joins, `UNPIVOT` keyword arguments, aliased
explodes, window reuse, denser pipelines). It recovered v3's classic accuracy and solves 5/5 window queries of the v4
hold-out set (incl. `RANGE` frames, named `WINDOW`, sessionization), `GROUPING SETS` over a join, `LATERAL ... LIMIT 1`,
the `<=>` join and `LATERAL VIEW OUTER EXPLODE`. It misses the promotion rule on two points (below); on 2026-09-28 it was
promoted to `latest` anyway by the owner's decision, with v3 kept as `sql2pyspark:v3` for rollback:

* **Hold-out 56/62**: besides v3's three failures and v4's `holdout058` (`.distinct()` dropped in a CROSS JOIN), it
  fails `holdout015` (`DATEDIFF(...) / 365` rendered as `... / 365.alias(...)`) and `holdout041` (no `result`).
* **Real-world queries 0/4**: `real001` still loops (`w1` … `w93` window definitions until the token limit), `real002`
  leaves a parenthesis open, `real004` drops a quote in a regex; `real003` parses and runs but names an `UNPIVOT` value
  `temperature` instead of `TEMPERATURE`. Two replies now parse (v4: 1).

### v4 results (2026-09-27)

| Eval set | v3 | v4 | Change |
|---|---|---|---|
| Hold-out (62, unseen tables) | 59/62 (95.2%) | 58/62 (93.5%) | -1: `holdout058` (dropped `.distinct()` on one side of a CROSS JOIN) |
| v4 hold-out (25, new features) | 3/25 (12%) | 9/25 (36%) | ×3: `FILTER`/`COUNT_IF`, `NULLS LAST` + `IGNORE NULLS`, `INTERVAL` join, `INTERSECT ALL`, multi-agg `PIVOT`, lambdas, struct/map/JSON |
| Real-world queries (4) | 0/4 | 0/4 | 1 reply now parses and runs up to an alias error (`real003`) |
| Test sample (150, v4 split) | 105/150 (70.0%) | 110/150 (73.3%) | On the 150-query test sample v4 is 5 queries ahead overall, but only because it gains 17 on the v4 families (26/53 vs. 9/53; long pipelines 5/22 vs. 0/22) while losing 12 on the classic v1–v3 families (84/97 vs. 96/97: 5 syntax errors, 3 runtime errors, 2 wrong column lists, 2 wrong row sets), so v4 is not a safe drop-in replacement either. |

**Promotion rule** (promote only if the new model scores at least the old one's hold-out result, beats it on the v4
hold-out and the real queries, and doesn't regress on the test sample): v4 misses the first and third condition, so
`sql2pyspark:latest` stayed on v3 at the time (v5 later replaced it). v4 stays available as `sql2pyspark:v4`.

**Why the real queries still fail:** `real001` sends v4 into a repetition loop (it defines the same window `w1`...`w93`
until the 2,048-token limit), `real002` turns the null-safe `<=>` into `=`, `real004` leaves a parenthesis unclosed, and
`real003` parses and runs but mixes up a column qualifier. These queries are 1,200-2,400 tokens long and combine ~15
features each; the v4 pipelines teach the individual features but rarely at that length and density.

**Repetition penalty (2026-09-28)**: Ollama's default repetition window (64 tokens) is too short to stop `real001`'s
window-definition loop. `repeat_last_n=2048` stops it at the cost of one hold-out query, and stronger `repeat_penalty`
values make the model misspell repeated column names (hold-out 49 and 41). No setting makes a real query correct.
`app.py` therefore uses `repeat_last_n=2048` only for SQL longer than 2,000 characters (details in docs/04).

**Agent repair with Kimi K3 (2026-09-28)**: `eval/agent_repair.py` gives Kimi K3 (Moonshot AI) each real query, the
model's answer and one tool that runs PySpark on the sample tables (no expected output, no `.sql(...)`, at most 5 runs).
On **v6** it fixes 3 of the 4 real queries in only **4 tool runs** (1 per query) for **$0.59** (on v5: 11 runs, $0.90).
`real003` still keeps NULL rows that Spark SQL's `UNPIVOT` drops by default. Needs `KIMI_API_KEY` in a `.env` file in
the repo root (gitignored; see [USER_MANUAL.md](USER_MANUAL.md#activating-kimi-k3); details in docs/04).

**Kimi K3 repair in the app**: with a key, the app statically flags syntax errors, undefined names, a missing `result`,
positional set-operation mismatches (`setop_check()`) and, since 2026-10-01, unqualified columns in a join condition
between aliased DataFrames (`join_check()`: `holdout024`, `holdout052`), letting Kimi K3 repair the code. The sidebar repair mode
defaults to *When a check fails or the SQL is long* (> 2,000 characters). With no data available, Kimi checks its code
on empty tables with schemas inferred from the SQL, comparing output columns with the SQL's projection (`kimi_repair.py`).
With explicit rules for `UNPIVOT` NULL exclusion and positional `UNION` matching it repaired v7's real-query answers
11/12 times on sample tables and **12/12** on empty tables over 3 runs per mode (~$0.18 per query; docs/04, docs/05).

**Known gaps (after v5)**: `real001`'s repetition loop, the long-standing `UNION` alias gaps (`holdout031`,
`holdout053`), `holdout050`, `holdout058`, pipelines in the v4 hold-out set (0/2) and on the fixed test set (8/22), and
two v5 slips on simple expressions (`holdout015`, `holdout041`). The v4 list below is kept for reference.

**Known gaps (v4)**, targets for the v5 dataset:

* **Classic-query regression**: on the test sample v4 loses 12 of the v1–v3 family queries that v3 gets right
  (84/97 vs. 96/97). The v4 run saw the same ~4,800 samples as v3, but only ~58% of them were classic families; v5 should
  train longer (or continue from v3's adapters) so the new families don't crowd out the old ones.
* Very long inputs: repetition loops on the longest query; long pipelines are v4's weakest test family (5/22). More
  training samples near 2-3k tokens (and more steps).
* `<=>` translated as `=`; parenthesis balance in long join conditions.
* `UNION` of differently named columns (`holdout031`) and garbled aliases in `UNION ALL` with literals (`holdout053`),
  still failing since v3.
* `holdout058`: `.distinct()` dropped on one side of a CROSS JOIN of two `SELECT DISTINCT` subqueries.
* Generator: some `HAVING agg < 0` filters and an inherited `NOT IN` over every value of a column produce empty results,
  which makes their Spark check weak (the pairs are still correct).
* Pipelines in the v4 hold-out (0/2) and windows (40%, unchanged) are the weakest groups.

## Docker

Everything except training runs in Docker (tested with Docker Desktop 29.8.0; both images use `python:3.12-slim-bookworm`):

```bash
docker compose up -d --build                                          # Streamlit UI on :8501, model from the host's Ollama
docker compose --profile tools run --rm tools make data PY=python     # dataset pipeline (Java 17 + PySpark)
docker compose --profile tools run --rm tools make eval PY=python TAG=v5 PREV=v3   # evaluation against the host's Ollama
OLLAMA_URL=http://ollama:11434 docker compose --profile ollama up -d --build      # Ollama in a container (Linux servers)
```

Verified: the app container, the v3 dataset build in `tools` (15,105/15,105 pairs), and the v3 hold-out evaluation in
`tools` (59/62, identical replies and scores to the native run). Training cannot run in Docker on a Mac (MLX needs
Metal, which containers can't use), and Ollama in a container is CPU-only there, so native Ollama stays the recommended
way to serve the model on macOS. Stop Docker before `make train`.

## Server deployment

The trained model also runs on a Linux server: **https://sql2pyspark.praponb.com** is served from a home Ubuntu machine
(`192.168.1.49`, CPU only, native Ollama with v9-q4, the Streamlit app as a systemd service, Cloudflare Tunnel for HTTPS,
**Cloudflare Access login**: owner's email only, sign in with Cloudflare or an emailed one-time code). The 7B model needs
~8–12 GB RAM as Q4_K_M (12–16 GB as Q8_0) and ~15–25 GB disk; without a GPU a short query takes ~25 s and a typical one up to a minute. `deploy/update_model.sh` copies a
newly promoted model to the server (`GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8` sent the 4.7 GB Q4_K_M build on 2026-10-01; on the server's CPU v9-q4 generates **4.05 tokens/s vs. v8's 2.47** (1.64×): the app's default GROUP BY query takes **23.5 s vs. 38.5 s** warm, and loading the model takes 16 s vs. 36 s (measured 2026-10-01)). Steps, sizing, updates and
troubleshooting: [docs/08-server-deployment.md](docs/08-server-deployment.md); service files in `deploy/`.

## Layout

```
app.py, kimi_repair.py, Modelfile, Makefile, requirements.txt, pyrefly.toml, .gitignore
README.md, USER_MANUAL.md, PROJECT_DOCUMENTATION.md, docs/          documentation
sql2pyspark-mlx/   schemas.py, sqlspec.py, generate_dataset.py, advanced.py (v4–v7 families), handwritten_pairs.py,
                   verify_pairs.py, convert_sql_pyspark.py, lora_config.yaml, train_resumable.py, train.log,
                   data_v7.log, raw_pairs/, data/, adapters/,
                   complex_v8/ (10k complex pairs & v8 LoRA config),
                   complex_v9/ (mirror window style, 13.2k mix & v9 LoRA config),
                   classic_v10/ (classic pairs in mirror style + IN / NOT IN pairs, 12.5k mix & v10 LoRA config),
                   runs/v2/ … runs/v7/ (adapters, configs, logs; v6/v7 data is in the git history too)
sql2pyspark.gguf   Q8_0 GGUF of the latest export (v10 since 2026-10-01, 8.1 GB); Ollama keeps its own copy of every tag
sql2pyspark-q4.gguf Q4_K_M GGUF of the same export (v10, 4.7 GB, compact CPU export for Linux server)
eval/              build_holdout.py, holdout.jsonl, build_holdout_v4.py, holdout_v4.jsonl, build_real_queries.py,
                   real_queries.jsonl, real_tables.py, test_fixed.jsonl, evaluate.py, report.py, agent_repair.py,
                   post_training_v4.sh … post_training_v8.sh, start_training_v7.sh … start_training_v10.sh,
                   complex_v8_test.jsonl, complex_eval_v8.sh,
                   results/ (+ v2-split/ … v8-repair/, v7-repair-rules/, v9-q4-repair/, inlist/), logs-repair-rules/, *.log
docker/, docker-compose.yml, .dockerignore          Docker setup
deploy/            sql2pyspark.service (systemd unit), setup_server.sh, update_model.sh: Ubuntu server hosting (docs/08)
llama.cpp/         upstream checkout, used only for convert_hf_to_gguf.py
```

The Docker `ollama` profile registers the model from the mounted `sql2pyspark.gguf`, i.e. the last export (v8, not `latest`).

`archive/` (earlier experiments and the v1 model/data) and the legacy `mlx-env/` venv were deleted on 2026-09-27.
`make export` also writes `sql2pyspark-hf/` (~15 GB BF16) and `sql2pyspark-mlx/sql2pyspark-model/` (~4 GB), and training
writes LoRA checkpoints every 200 steps; once the export is in Ollama, `make clean` deletes all of these (and `__pycache__/`)
and keeps `sql2pyspark.gguf` and each run's final `adapters.safetensors`.

Cleanup on 2026-09-30: those leftovers (~21.6 GB), the Ollama tags `sql2pyspark:v1` … `:v6` (~24.6 GB; the Mac keeps
`latest` = v8 and v7) and the v2–v5 datasets in `runs/` (~760 MB, still in the git history) were removed. v2–v6 can be
rebuilt from their adapters: `mlx_lm.fuse --adapter-path runs/vN/adapters` (v2–v5 with `--model mlx-community/Qwen2.5-Coder-3B-Instruct-4bit`), then `make gguf ollama TAG=vN`.

The code is in the private GitHub repo `praponb/Finetuning-LLM`; `.gitignore` keeps large model binaries (`*.gguf`, `sql2pyspark-hf/`, `sql2pyspark-model/`), venvs, and `llama.cpp/` out of git, while data, raw pairs, run logs, configs, and adapters are tracked in plain git.
