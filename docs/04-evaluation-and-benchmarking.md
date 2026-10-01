# 04 - Evaluation & Benchmarking

Text-similarity metrics such as BLEU or CodeBLEU are unreliable for code: two functionally identical PySpark scripts can look very different, while one misplaced character can make code invalid.

This project therefore uses **execution-based evaluation** in local Apache Spark.

---

## 🎯 Evaluation Methodology

For each test case, the original SQL and the model-generated PySpark are both executed in Spark, and the results are compared:

$$\text{Evaluate}(SQL, LLM) \implies \text{Spark.SQL}(SQL) \equiv \text{Spark.Exec}(LLM(SQL))$$

```
                          ┌──────────────────────────┐
                          │   Test Query (SQL)       │
                          └─────────────┬────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
        ┌───────────────────────┐               ┌───────────────────────┐
        │  Ollama Inference     │               │  Spark SQL Execution  │
        │  (OLLAMA_URL)         │               │  spark.sql(sql)       │
        └───────────┬───────────┘               └───────────┬───────────┘
                    │                                       │
                    ▼                                       ▼
        ┌───────────────────────┐                     Ground-Truth
        │  extract_code()       │                     Columns & Rows
        └───────────┬───────────┘                           │
                    │                                       │
                    ▼                                       │
        ┌───────────────────────┐                           │
        │  PySpark Execution    │                           │
        │  exec(compile(code))  │                           │
        └───────────┬───────────┘                           │
                    │                                       │
                    ▼                                       │
               Model Result                                 │
               Columns & Rows                               │
                    │                                       │
                    └───────────────────┬───────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │   verify_pairs.compare()    │
                         │ 1. Syntax Check (AST)       │
                         │ 2. Columns Match?           │
                         │ 3. Multiset Rows Match?     │
                         │ 4. Order Sequence Match?    │
                         └─────────────────────────────┘
```

Queries are sent with temperature 0 and a fixed seed, so runs are repeatable.

---

## 🧪 The Hold-Out Benchmark (`eval/holdout.jsonl`)

Built by [eval/build_holdout.py](../eval/build_holdout.py), the hold-out set has **62 hand-written queries** over tables the model **never saw during training**:
- `patients`, `doctors`, `appointments` (healthcare)
- `flights`, `bookings` (aviation)

Every reference answer passes the execution check with a non-empty result. Because the table and column names are new and the SQL is hand-written, this set tests generalisation, not memorization of the training schema or the generator's style.

### Difficulty Breakdown:
1. **Simple (15)**: single-table selects and filters (`BETWEEN`, `IN`, `LIKE`, `IS NULL`), `DISTINCT`, string and date functions (`UPPER`, `TRIM`, `YEAR`, `HOUR`, `TO_DATE`, `DATEDIFF`), `CASE WHEN`, `COALESCE`, global aggregates, `ORDER BY ... LIMIT`.
2. **Medium (22)**: `GROUP BY` / `HAVING` / `ORDER BY`, conditional aggregates, `COUNT(DISTINCT)`, inner and left joins, `IN` / `NOT EXISTS` subqueries, scalar subqueries, `RANK`, `LAG`, `DATE_TRUNC`, `UNION`.
3. **Complex (25)**: CTEs (including chained CTEs with a left join), top-N per group, running totals and share-of-total windows over aggregates, three-way joins, anti-joins via `LEFT JOIN ... IS NULL`, correlated subqueries, `PIVOT`, `ROLLUP`, `CROSS JOIN`, `INTERSECT` / `EXCEPT`, `UNION ALL` with literals, moving averages, `PERCENT_RANK`, `GROUP BY 1` on a `CASE`.

## 🌍 Real-World Queries (`eval/real_queries.jsonl`)

Built by [eval/build_real_queries.py](../eval/build_real_queries.py) from SQL that users actually run. The queries use their own tables, so [eval/real_tables.py](../eval/real_tables.py) defines small hand-made sample tables for them (`make_dataframes(spark)`), and the evaluator loads them with `--tables`. The builder checks every reference answer against its SQL in Spark (non-empty result) before writing the set.

Current contents (4 queries, updated 2026-09-27):

| id | Query | Tables | What it exercises |
| :--- | :--- | :--- | :--- |
| `real001` | Marketing attribution report, 4 CTEs | `raw_events`, `marketing_campaigns`, `fraud_blacklist` | Dedup with `ROW_NUMBER() ... NULLS LAST`, `LAG`/`LEAD`, 30-minute sessionization, non-equi `LEFT JOIN` with `INTERVAL` arithmetic, running and `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` frames, `DENSE_RANK`, conditional aggregates with `NULLIF`, `HAVING`, correlated `NOT EXISTS` with a date condition, `GROUP BY GROUPING SETS`, `PERCENT_RANK` over an aggregate, `ORDER BY ... NULLS LAST` |
| `real002` | Order fulfillment & warehouse routing | `orders`, `warehouse_inventory`, `blacklisted_fulfillments`, `warehouse_sla` | Higher-order array lambdas (`TRANSFORM`, `FILTER`), `LATERAL VIEW OUTER EXPLODE`, epoch-based `RANGE BETWEEN` window frames, null-safe equality (`<=>`), `QUALIFY` clause filtering, set difference (`EXCEPT DISTINCT` / `subtract`), native SQL `PIVOT`, correlated scalar subquery in SELECT, regex pattern matching (`RLIKE`) |
| `real003` | IoT telemetry & sensor calibration | `sensor_telemetry`, `calibration_registry`, `operational_incidents`, `device_metadata` | Complex JSON parsing (`FROM_JSON`), bitwise manipulation (`&` and `BIT_OR`), native SQL `UNPIVOT`, correlated `LATERAL` join with `LIMIT 1`, stateful window imputation (`LAST_VALUE IGNORE NULLS`), `CUME_DIST`, `NTILE`, `FULL OUTER JOIN`, `MAP_FROM_ARRAYS`, ANSI aggregate `FILTER` |
| `real004` | Trade execution analytics & settlement | `trade_stream`, `cleared_settlement_batches` | `REGEXP_EXTRACT`, safe casting (`TRY_CAST`), higher-order array `ZIP_WITH` and `AGGREGATE` (fold/reduce), sessionization gaps-and-islands (`LAG` + `INTERVAL`), reusable named `WINDOW` specifications, `STDDEV_SAMP`, `CORR`, multiset intersection (`INTERSECT ALL`), multi-dimensional `CUBE` aggregation, `GROUPING_ID`, `APPROX_PERCENTILE` |

The sample tables are designed so that every clause changes the result, and reference PySpark implementations are execution-verified against Spark SQL before writing to JSONL.

`real003` and `real004` also carry a `ref_sql`. Spark 4.2 rejects two pieces of the original syntax: string-literal `UNPIVOT` aliases (`temp_celsius AS 'TEMPERATURE'`) and the repeated `GROUPING_ID(... CASE ...)` in `HAVING`. The evaluator computes the expected result from `ref_sql` (the same query with those spots rewritten: `AS TEMPERATURE`, `HAVING grouping_tier_bitmask < 7`) while the model always gets the user's original `sql`.

To add a query: append an `R(name, sql, reference_code, order_cols[, ref_sql])` entry in `build_real_queries.py`, add any new tables to `real_tables.py`, and run `python eval/build_real_queries.py`.

---

## 🧩 The v4 Hold-Out Benchmark (`eval/holdout_v4.jsonl`)

Built by [eval/build_holdout_v4.py](../eval/build_holdout_v4.py): **25 hand-written queries** on the hold-out tables plus the nested hold-out table `vehicle_trips`, each targeting features that v1–v3 never saw in training. The builder checks every reference against its SQL in Spark (non-empty result). `level` is the feature group:

| Group | n | Examples |
| :--- | :--- | :--- |
| aggregation | 4 | `GROUPING SETS` over a join, `ROLLUP` + `GROUPING_ID` + `HAVING`, `FILTER (WHERE ...)` / `COUNT_IF` / `NULLIF` ratio, `PERCENTILE_APPROX` + `SORT_ARRAY(COLLECT_SET)` + `PERCENT_RANK` over `AVG` |
| window | 5 | `DENSE_RANK ... NULLS LAST` + `LAST_VALUE IGNORE NULLS`, epoch `RANGE` frame + `ROWS 3 PRECEDING AND 1 PRECEDING`, named `WINDOW` + `LAG` default + `CUME_DIST`, `QUALIFY`, 6-hour shift sessionization |
| join | 5 | `INTERVAL` range join, `LEFT JOIN LATERAL ... LIMIT 1`, `NOT EXISTS` with a correlated date condition, `<=>` join, correlated scalar subquery + `RLIKE` |
| setop | 3 | `UNION` of differently named columns with literals + `ORDER BY` (the v3 gap), `EXCEPT ALL`, `INTERSECT ALL` |
| reshape | 2 | multi-aggregate `PIVOT` with value aliases, `UNPIVOT` with aliases |
| nested | 4 | `LATERAL VIEW OUTER EXPLODE` of an array of structs, `POSEXPLODE`, `AGGREGATE/TRANSFORM/FILTER/EXISTS/ZIP_WITH` lambdas, struct / map / `TRY_CAST` / `GET_JSON_OBJECT` / `FROM_JSON` |
| pipeline | 2 | 4-CTE appointment report (filter → windows → per-doctor `FILTER` aggregates → join + `RANK`), explode → daily aggregates → `DENSE_RANK` |

---

The second benchmark is a random sample of 300 queries from the generated test split (`sql2pyspark-mlx/data/test_meta.jsonl`), whose feature signatures never occur in training.

---

## 🔬 Scoring Modes: Strict vs. Lenient

[eval/evaluate.py](../eval/evaluate.py) scores every answer both ways:

### 1. Strict Mode (the v2 convention)
- Code is executed **exactly as returned**.
- It must import what it uses (`from pyspark.sql import functions as F`), read from `df_<table_name>`, and assign the final DataFrame to `result`.
- Otherwise it fails (`no_result`, `py_error` or `syntax`).

### 2. Lenient Mode (fair baseline for v1)
The v1 model was trained without these conventions (it writes `df_result = ...` and usually imports `functions as f`). To score it fairly, lenient mode:
- prepends the standard imports:
  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window
  ```
- binds a bare `df` to the query's main table (`df = df_<main_table>`),
- makes the last expression or assignment the `result`.

---

## 📊 Benchmark Results

Generated by [eval/report.py](../eval/report.py) from `eval/results/*.json` (rows grouped by eval set):

| Model | Eval set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
|---|---|---|---|---|---|---|---|---|
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v4` | holdout | 62 | 100% | 95% | **94%** | 95% | **94%** | 4.0 |
| `sql2pyspark:v5` | holdout | 62 | 97% | 92% | **90%** | 92% | **90%** | 4.3 |
| `sql2pyspark:v6` | holdout | 62 | 100% | 97% | **97%** | 97% | **97%** | 9.9 |
| `sql2pyspark:v7` | holdout | 62 | 100% | 98% | **98%** | 98% | **98%** | 9.1 |
| `sql2pyspark:v8` | holdout | 62 | 100% | 97% | **97%** | 97% | **97%** | 9.3 |
| `sql2pyspark:v9` | holdout | 62 | 100% | 95% | **95%** | 95% | **95%** | 7.3 |
| `sql2pyspark:v9-q4` | holdout | 62 | 100% | 95% | **95%** | 95% | **95%** | 8.3 |
| `sql2pyspark:v2` | holdout_v4 | 25 | 92% | 32% | **20%** | 32% | **20%** | 6.5 |
| `sql2pyspark:v3` | holdout_v4 | 25 | 88% | 32% | **12%** | 32% | **12%** | 7.1 |
| `sql2pyspark:v4` | holdout_v4 | 25 | 96% | 60% | **36%** | 60% | **36%** | 7.0 |
| `sql2pyspark:v5` | holdout_v4 | 25 | 88% | 68% | **56%** | 68% | **56%** | 8.1 |
| `sql2pyspark:v6` | holdout_v4 | 25 | 100% | 80% | **64%** | 80% | **64%** | 18.4 |
| `sql2pyspark:v7` | holdout_v4 | 25 | 100% | 80% | **64%** | 80% | **64%** | 16.4 |
| `sql2pyspark:v8` | holdout_v4 | 25 | 100% | 76% | **72%** | 76% | **72%** | 17.1 |
| `sql2pyspark:v9` | holdout_v4 | 25 | 100% | 76% | **72%** | 76% | **72%** | 13.3 |
| `sql2pyspark:v9-q4` | holdout_v4 | 25 | 100% | 84% | **84%** | 84% | **84%** | 16.2 |
| `sql2pyspark:v2` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 55.3 |
| `sql2pyspark:v3` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 78.9 |
| `sql2pyspark:v4` | real_queries | 4 | 25% | 0% | **0%** | 0% | **0%** | 92.8 |
| `sql2pyspark:v5` | real_queries | 4 | 50% | 0% | **0%** | 0% | **0%** | 75.6 |
| `sql2pyspark:v6` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 189.4 |
| `sql2pyspark:v7` | real_queries | 4 | 50% | 0% | **0%** | 0% | **0%** | 163.6 |
| `sql2pyspark:v8` | real_queries | 4 | 100% | 0% | **0%** | 0% | **0%** | 195.5 |
| `sql2pyspark:v9` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 131.2 |
| `sql2pyspark:v9-q4` | real_queries | 4 | 75% | 0% | **0%** | 0% | **0%** | 164.5 |
| `sql2pyspark:v3` | test_fixed | 150 | 92% | 73% | **69%** | 73% | **70%** | 8.0 |
| `sql2pyspark:v4` | test_fixed | 150 | 90% | 80% | **73%** | 80% | **73%** | 11.6 |
| `sql2pyspark:v5` | test_fixed | 150 | 97% | 88% | **85%** | 89% | **85%** | 11.2 |
| `sql2pyspark:v6` | test_fixed | 150 | 99% | 91% | **88%** | 91% | **88%** | 22.5 |
| `sql2pyspark:v7` | test_fixed | 150 | 100% | 93% | **91%** | 93% | **91%** | 25.8 |
| `sql2pyspark:v8` | test_fixed | 150 | 99% | 96% | **93%** | 96% | **93%** | 24.3 |
| `sql2pyspark:v9` | test_fixed | 150 | 99% | 94% | **89%** | 94% | **89%** | 19.4 |
| `sql2pyspark:v9-q4` | test_fixed | 150 | 100% | 97% | **93%** | 97% | **93%** | 18.6 |
| `sql2pyspark:v7` | complex_v8_test | 100 | 83% | 38% | **31%** | 38% | **31%** | 72.9 |
| `sql2pyspark:v8` | complex_v8_test | 100 | 96% | 68% | **60%** | 68% | **60%** | 96.9 |
| `sql2pyspark:v9` | complex_v8_test | 100 | 100% | 92% | **87%** | 92% | **87%** | 57.9 |
| `sql2pyspark:v9-q4` | complex_v8_test | 100 | 99% | 91% | **87%** | 91% | **87%** | 39.2 |

Hold-out, correct results (lenient) by difficulty:

| Model | simple (n=15) | medium (n=22) | complex (n=25) |
|---|---|---|---|
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | 100% | 95% | 92% |
| `sql2pyspark:v4` | 100% | 95% | 88% |
| `sql2pyspark:v5` | 93% | 95% | 84% |
| `sql2pyspark:v6` | 93% | 95% | 100% |
| `sql2pyspark:v7` | 100% | 95% | 100% |
| `sql2pyspark:v8` | 100% | 91% | 100% |

v4 hold-out, correct results (lenient) by feature group:

| Model | aggregation (n=4) | window (n=5) | join (n=5) | setop (n=3) | reshape (n=2) | nested (n=4) | pipeline (n=2) |
|---|---|---|---|---|---|---|---|
| `sql2pyspark:v2` | 0% | 40% | 20% | 67% | 0% | 0% | 0% |
| `sql2pyspark:v3` | 0% | 40% | 0% | 33% | 0% | 0% | 0% |
| `sql2pyspark:v4` | 25% | 40% | 20% | 67% | 50% | 50% | 0% |
| `sql2pyspark:v5` | 50% | 100% | 40% | 67% | 50% | 50% | 0% |
| `sql2pyspark:v6` | 75% | 80% | 60% | 67% | 50% | 50% | 50% |
| `sql2pyspark:v7` | 50% | 80% | 60% | 33% | 100% | 75% | 50% |
| `sql2pyspark:v8` | 100% | 60% | 60% | 67% | 100% | 75% | 50% |

updated README.md

### Test split: the fixed test set (`eval/test_fixed.jsonl`)
The generated test split is recomputed for every dataset version, so each version has different test queries. Since v5 the comparison uses a **fixed test set**: the 150-query sample of the v4 test split (seed 0), saved as `eval/test_fixed.jsonl` and removed from later training data (`convert_sql_pyspark.py --exclude`). v3's and v4's stored replies were re-scored on it with identical results, so v3, v4 and v5 are compared on exactly the same queries. Of the 150, 97 come from the classic v1–v3 families and 53 from the v4 families (22 pipelines).

Earlier samples are kept outside the report's glob: `eval/results/v4-split/` (the same 150 queries, v3/v4 files before the rename), `v3-split/` (300 queries of the v3 split: v2 286, v3 284) and `v2-split/` (v1 **8%**, v2 **98%**).

### Promotions
- **v2** beat v1 on hold-out correctness (90% vs. 6%) and was promoted to `sql2pyspark:latest`.
- **v3** beat v2 on the hold-out set (95.2% vs. 90.3%: it fixes 3 of v2's 6 failures with no regressions) and tied it on the same test sample, so it was promoted on 2026-09-26 (`make promote TAG=v3 PREV=v2`). v2 stays available as `sql2pyspark:v2`.
- **v4** (2026-09-27) was **not promoted**; see [v4 results](#-v4-results-2026-09-27). It stays available as `sql2pyspark:v4`.
- **v5** (2026-09-28) missed the rule on two conditions (see [v5 results](#-v5-results-2026-09-28)) but was **promoted to `latest` by the owner's decision** (`make promote TAG=v5 PREV=v3`); v3 is kept as `sql2pyspark:v3` for rollback.
- **v7** (2026-09-29, 7B continued from v6) beats or ties v6 on every set but ties it on the v4 hold-out (16) and, like every model, solves no real-world query; the owner **promoted** it on 2026-09-29 (`make promote TAG=v7 PREV=v6`); v6 is kept as `sql2pyspark:v6`. See [v7 results](#-v7-results-2026-09-29).
- **v8** (2026-09-30, continued from v7 on the complex_v8 mix) is better on the v4 hold-out (18 vs. 16), the fixed test set (139 vs. 137) and the new complex test (60 vs. 31), but scores 60/62 on the hold-out (rule ≥ 61) and 0/4 real-world queries; the owner **promoted** it on 2026-09-30 (`make promote TAG=v8 PREV=v7`); v7 is kept as `sql2pyspark:v7`. See [v8 results](#-v8-results-2026-09-30).
- **v6** (2026-09-28, the 7B model) beats or ties v5 on every set and meets every condition except the real-world query one, which no model has met; it was **promoted** (`make promote TAG=v6 PREV=v5`), with v5 kept as `sql2pyspark:v5` for rollback. See [v6 results](#-v6-results-2026-09-28).

The rule (since v5): promote a new model only if it scores at least the current one on the hold-out set (≥ 59/62), keeps its accuracy on the classic-family queries of the fixed test set (≥ v3 − 2, i.e. ≥ 94/97), beats it on the v4 hold-out set, gets at least 1 of the 4 real-world queries right, and doesn't regress on the fixed test set overall. (v4 used the same rule without the classic-family condition and with "better on the real queries".)

### Why v1 fails (hold-out, lenient mode)
Of 62 answers, 4 were correct. The rest broke down as:
- **Runtime errors (38)**: `TypeError` (11, e.g. `'Column' object is not callable`), `AnalysisException` (10, e.g. `.orderBy("doctor_count desc")`, bad `USING` joins), `NameError` (9, e.g. bare `desc`, `col`), `AttributeError` (6, e.g. `GroupedData.withColumn`).
- **Syntax errors (13)**: mostly unbalanced parentheses.
- **Wrong columns (4) or rows (2)**, and 1 answer with no DataFrame.

It also never followed the v2 conventions: all 62 answers assign `df_result`, and 59 import `functions as f`. v1 was trained on ~24 fixed templates over one schema, so it had not learned how to handle new tables or query shapes.

### v2 hold-out failures and what v3 changed
v2 failed 6 of 62 hold-out queries:
- 2 × `UNION` / `UNION ALL` of differently named columns (`city` ∪ `hospital`, `full_name` ∪ `passenger_name`): it used `unionByName`, which fails. The v2 generator only produced set operations over identically named columns.
- 1 × `GROUP BY 1` where column 1 is a `CASE`: it grouped by the raw `YEAR(...)` instead.
- 2 × unbalanced parentheses in long expressions (`SUM(SUM(x)) OVER ()`, `DATEDIFF(...) / 365`).
- 1 × a made-up join type (`"left_seats"`) on an `EXISTS` with three conditions.

The v3 dataset added set operations over differently named columns, `GROUP BY` ordinals on `CASE` keys and exact `NOT IN` semantics (see [02 - Data Engineering](02-data-engineering-pipeline.md#v3-generator-changes)). v3 fixes the `GROUP BY 1` case and both parenthesis slips.

### Where v3 still fails
**Hold-out (3 of 62):**
- `holdout031`: `SELECT city ... UNION SELECT hospital ...` still uses `unionByName` instead of `union` (`UNRESOLVED_COLUMN`).
- `holdout050`: `EXISTS` with three conditions → `left_semi` join, now with a misplaced closing parenthesis (syntax error; v2 made up a join type here).
- `holdout053`: `UNION ALL` with literal `kind` columns and aliases: garbled alias syntax (`"full_name.alias("name")`, syntax error).

**Test sample (16 of 300):** 8 are set operations (see below); the others are spread over `agg` (3), `select` (2), `special`, `subquery` and `topn` (1 each): 5 runtime errors, 1 wrong column list, 1 wrong ORDER BY, 1 wrong rows.

**Real-world queries (`real001`–`real004`)**: both v2 and v3 fail all 4 queries; neither reply parses into valid executable PySpark (0% syntax / 0% match).
- **`real001` (marketing attribution)**: 2,371 tokens as a training sample (training: max 603). Both models invent functions (`F.groupingSets`, `F.interval`, `F.nullIf`), misplace parentheses in complex interval join clauses, or flatten multiple distinct CTE stages into invalid single chains.
- **`real002` (order fulfillment)**: Exercises higher-order array transforms (`TRANSFORM`, `FILTER`), `LATERAL VIEW OUTER EXPLODE`, `RANGE BETWEEN` epoch seconds, `QUALIFY`, and native `PIVOT`. Models emit invalid Python lambda expressions or invent non-existent DataFrame methods.
- **`real003` (iot telemetry)**: Involves `FROM_JSON` schema extraction, bitwise operators (`&`, `BIT_OR`), SQL `UNPIVOT`, correlated `LATERAL` joins, and `LAST_VALUE IGNORE NULLS`. Models fail to construct valid PySpark UNPIVOT calls and mishandle bitwise masking.
- **`real004` (trade analytics)**: Involves `TRY_CAST`, higher-order `ZIP_WITH` and `AGGREGATE` (fold/reduce), sessionization gaps-and-islands, named windows, and `CUBE` with `GROUPING_ID`. Models hallucinate syntax for array aggregation and produce unparseable expressions.

The queries are far outside the v3 training distribution:

| Property of Real-World Suite | Training data (12,743 samples) |
| :--- | :--- |
| Length: 1,200–2,400 tokens | median 210, max 603 tokens; `max_seq_length: 1024` |
| Multi-CTE pipelines (3–5 CTEs) | at most 2 |
| `GROUPING SETS` | 0 samples (`CUBE`: 26) |
| `UNPIVOT` and `QUALIFY` | 0 samples |
| Higher-order array functions (`TRANSFORM`, `FILTER`, `ZIP_WITH`, `AGGREGATE`) | 0 samples |
| Bitwise operations (`&`, `BIT_OR`) | 0 samples |
| `INTERVAL` arithmetic | 0 samples |
| `NULLS LAST` | 1 sample |
| `NULLIF` | 5 samples |

**Shared gap:** all 9 set-operation queries in the test sample share one held-out signature: `UNION` + a literal `source` column + `ORDER BY`. Both models drop the `.distinct()` after `unionByName`, returning ~80 rows instead of ~8 (v2 3/9 correct, v3 1/9). This signature never occurs in training, so it is the main target for a v4 dataset, together with the two hold-out `UNION` failures.

---

## 🆕 v9 results (2026-10-01)

v9 continues from v8's adapters on **`complex_v9/data_mix`**: the complex_v8 queries re-rendered in the **"mirror" window style** (see [Data Engineering § 8](02-data-engineering-pipeline.md)) — 7,251 pairs with windows (incl. 1,990 extra `cx_window_heavy` pairs) + 2,000 complex pairs without windows + 4,000 v7 pairs = 13,251; 2,400 micro-batches of 1 (600 updates of 4, peak LR 5e-5, cosine to 5e-6), 2026-09-30 19:05 → 10-01 05:23 (10 h 18 min, peak 10.0 GB). Validation loss 0.034 → 0.018 (step 800) → 0.007 (1,600) → **0.008**. The same f16 export was quantized twice with `llama-quantize`: Q8_0 → `sql2pyspark:v9` (8.1 GB, as v6–v8) and Q4_K_M → `sql2pyspark:v9-q4` (4.7 GB). Both were scored by `eval/start_training_v9.sh` on every set (`eval/results/v9__*.json`, `v9-q4__*.json`).

| Eval set | v8 | v9 (Q8_0) | **v9-q4 (Q4_K_M)** | Rule (vs. v8) |
| :--- | :--- | :--- | :--- | :--- |
| Hold-out (62) | 60 | 59 | **59** | ❌ ≥ 60 (both miss by 1) |
| Fixed test set: classic families (97) | 97 | 96 | **97** | v9 ❌, v9-q4 ✅ |
| Fixed test set: pipelines (22) | 13 | 9 | **14** | — |
| Fixed test set: all (150) | 139 | 134 | **140** | v9 ❌, v9-q4 ✅ ≥ 139 |
| v4 hold-out (25) | 18 | 18 | **21** | v9 tie, v9-q4 ✅ |
| Real-world queries (4) | 0 | 0 | 0 | ❌ ≥ 1 |
| complex_v8 test, same 100 of 300 | 60 | 87 | **87** | ✅ |
| … `cx_window_heavy` (15) / `cx_pipeline` (32) / `cx_setop` (25) | 1 / 15 / 21 | 11 / 26 / 22 | 12 / 27 / 20 | — |
| Seconds per query: hold-out / fixed test / real / complex | 9.3 / 24.3 / 196 / 97 | 7.3 / 19.4 / 131 / 58 | 8.3 / 18.6 / 165 / 39 | — |

- **Many-window queries 1/15 → 11/15 (v9-q4 12/15)**: the change of target style fixed the failure mode seen in v8's answers (a hoisted block of near-identical `w_...` definitions repeated until the token limit, undefined names, wrong frames). Pipelines on the complex test also gained (15 → 26/27 of 32) and every family is at least as good as v8 except `cx_setop` on v9-q4 (20 vs. 21).
- **Hold-out**: both builds fail `holdout024`, `holdout031` and, new, `holdout052` (`NOT IN (subquery)` → `left_anti` join with an unqualified `F.col("id")`, ambiguous because both sides have `id`). `join_check()` in `kimi_repair.py` (added with v9) flags `holdout024` and `holdout052`; it flags none of the 541 reference answers in the eval sets.
- **Real-world queries**: still 0/4. `real001` still loops (`w70`, `w71`, … the same running-sum spec) — the real queries are classic-table SQL, where the training targets (the 4,000 v7 pairs) still hoist windows, so the mirror style did not transfer.
- **Q4_K_M vs. Q8_0**: on these sets the 4-bit build is equal or better everywhere (v4 hold-out +3, fixed test +6). With 25 and 150 queries that is within noise rather than a real gain, but it shows the 4-bit build loses nothing measurable here. It is faster on the Mac too (39 vs. 58 s per complex query); on the Ubuntu server's CPU it generates 1.64× as fast as v8 (4.05 vs. 2.47 tokens/s).
- **Decision**: v9 misses the rule on the hold-out and the fixed test; **v9-q4 misses only the hold-out, by `holdout052`**. The owner promoted **v9-q4** on 2026-10-01 (`make promote TAG=v9-q4 PREV=v8`) and deployed it to the server, where it generates 4.05 tokens/s vs. v8's 2.47 (default GROUP BY query: 23.5 s vs. 38.5 s warm).

---

## 🆕 v8 results (2026-09-30)

v8 continues from v7's adapters on **`complex_v8/data_mix`**: the new standalone [complex_v8 dataset](02-data-engineering-pipeline.md#7-the-complex_v8-dataset-v8) (10,000 verified multi-stage pairs on 20 new tables) plus 7,000 random v7 training pairs (800 updates of 4 samples as 3,200 micro-batches of 1, peak LR 5e-5, cosine to 5e-6; 2026-09-29 15:05 → 02:47, 11 h 42 min, peak 10.0 GB). Validation loss (1,000 samples: 500 new + 500 v7): **0.135** at the start (v7 had never seen this kind of query) → 0.021 (step 800) → 0.016 → 0.012 → **0.011**. Results (`eval/results/v8__*.json`, logs `eval/post_training_v8.log`, `eval/complex_eval_v8.log`):

| Eval set | v6 | v7 | **v8** | Rule (vs. v7) |
| :--- | :--- | :--- | :--- | :--- |
| Hold-out (62) | 60 | 61 | **60** (96.8%) | ❌ ≥ 61 (misses by 1) |
| Fixed test set: classic families (97) | 96 | 97 | **97** | ✅ ≥ 94 |
| Fixed test set: v4 families (31) / pipelines (22) | 26 / 10 | 28 / 12 | **29 / 13** | — |
| Fixed test set: all (150) | 132 | 137 | **139** (92.7%) | ✅ ≥ 137 |
| v4 hold-out (25) | 16 | 16 | **18** (72%) | ✅ > 16 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ ≥ 1 |
| **complex_v8 test, 100 of 300 (new)** | — | 31 | **60** | — |
| Seconds per query: hold-out / fixed test / real / complex | 9.9 / 22.5 / 189 / — | 9.1 / 25.8 / 164 / 73 | 9.3 / 24.3 / 196 / 97 | — |

- **Hold-out**: fails `holdout031` (still `.unionByName()` for the positional `UNION` of `city` and `hospital`) and, new, `holdout024`: a plain `IN (subquery)` where v8 writes `F.col("id") == F.col("patient_id")` in a `left_semi` join without the table aliases, so Spark reports `id` as ambiguous (v7 qualified both columns). Simple 100%, medium 91%, complex 100%.
- **v4 hold-out**: gains `holdout_v4_002`, `004`, `014`, `017`, `021`; loses `006`, `011`, `023`. Aggregation 4/4, windows 3/5, joins 3/5, set operations 2/3, reshape 2/2, nested 3/4, pipelines 1/2.
- **Fixed test set**: 6 right that v7 misses (4 pipelines, 1 aggregation, 1 nested), 4 wrong that v7 gets right (3 pipelines, 1 window); every classic query still correct.
- **complex_v8 test** (`eval/complex_v8_test.jsonl`: the 300 held-out-signature test pairs of the new dataset, 100 sampled with seed 0, tables from `complex_v8/eval_tables.py`): set operations **21/25** (v7 4/25), pipelines 15/32 (8), sessions 11/12 (9), grouping sets 4/5 (2), range joins 3/3 (2), PIVOT/UNPIVOT 5/8 (6), **many-window queries 1/15** (0). Code runs 68% (v7 38%), parses 96% (83%). Caveat: these queries come from the same generator and tables as the training data (only the feature combinations are unseen), so this gain is optimistic; the hold-out sets are the independent measure.
- **Real-world queries**: all 4 replies now parse (v7: 2), but none runs: `real001` never assigns `result`, `real002` refers to an alias `iv` that was never defined, `real003` calls `.rows` on a `WindowSpec`, `real004` uses `F.try_cast` (not a PySpark function). With the Kimi K3 repair: **4/4 with sample tables ($0.91) and 4/4 with empty tables ($0.81)**, the first model to reach 4/4 in both modes (`eval/results/v8-repair/`).

**Decision**: promoted by the owner on 2026-09-30. v8 is better on the v4 hold-out (+2), the fixed test set (+2, classic unchanged) and the new complex test (+29), but misses the hold-out condition by one query (a column-qualification slip that the app's check + repair loop fixes) and, like every model, the real-world condition. Promoted with `make promote TAG=v8 PREV=v7` and put on the Ubuntu server with `deploy/update_model.sh v8 v7`; roll back with `ollama cp sql2pyspark:v7 sql2pyspark:latest`.

## 🆕 v7 results (2026-09-29)

v7 continues from v6's adapters (600 updates of 4 samples as 2,400 micro-batches of 1, peak LR 5e-5, cosine to 5e-6; 18:13 → 23:11, 4 h 58 min) on the v7 dataset, which adds three generator patterns aimed at v6's failures (see [Data Engineering](02-data-engineering-pipeline.md#v7-generator-changes)): `DATEDIFF(...)` arithmetic (321 verified pairs), `UNION`s of unrelated columns with `IS NOT NULL` filters (144 + 133) and windows partitioned by a date expression (1,578; none before). Validation loss: 0.011 → 0.021 (step 800, after warm-up to the peak rate) → 0.010 → **0.009**. Results (`eval/results/v7__*.json`, log `eval/post_training_v7.log`):

| Eval set | v5 | v6 | **v7** | Rule (vs. v6) |
| :--- | :--- | :--- | :--- | :--- |
| Hold-out (62) | 56 | 60 | **61** (98.4%) | ✅ ≥ 60 |
| Fixed test set: classic families (97) | 95 | 96 | **97** | ✅ ≥ 94 |
| Fixed test set: v4 families (31) / pipelines (22) | 25 / 8 | 26 / 10 | **28 / 12** | — |
| Fixed test set: all (150) | 128 | 132 | **137** (91.3%) | ✅ ≥ 132 |
| v4 hold-out (25) | 14 | 16 | 16 | ❌ > 16 (tie) |
| Real-world queries (4) | 0 | 0 | 0 | ❌ ≥ 1 |
| Seconds per query: hold-out / fixed test / real | 4.3 / 11.2 / 76 | 9.9 / 22.5 / 189 | 9.1 / 25.8 / 164 | — |

- **Hold-out**: only `holdout031` fails (still `.unionByName()` for the positional `UNION` of `city` and `hospital`, despite 144 training pairs of unrelated-column UNIONs). `holdout015` is fixed, but the new `DATEDIFF` arithmetic targeted it directly, so that gain is optimistic. By difficulty: simple 100%, medium 95%, complex 100%.
- **Fixed test set** (not targeted by the new data): 7 queries right that v6 misses (3 pipelines, 3 aggregations, 1 nested query), 2 wrong that v6 gets right; every classic query correct.
- **v4 hold-out**: aggregation 2/4, windows 4/5, joins 3/5, set operations 1/3, reshape 2/2, nested 3/4, pipelines 1/2; it gains `holdout_v4_019` and `holdout_v4_023` and loses `holdout_v4_002` and `holdout_v4_017`.
- **Real-world queries**: `real001` still loops (88 window definitions), so the expression-partitioned windows did not remove the loop; only 2 of 4 replies parse (v6: 3), e.g. `real002` calls `asc_nulls_last` on a `WindowSpec`. With the Kimi K3 repair: 4/4 (sample tables) and 3/4 (empty tables).

**Decision**: v7 beats or ties v6 everywhere; it misses the v4-hold-out condition by a tie and the real-world condition that no model has met (v6 was promoted with the latter). The owner promoted it on 2026-09-29 (`make promote TAG=v7 PREV=v6`; the unattended session that produced it was not allowed to change `latest`). Roll back with `ollama cp sql2pyspark:v6 sql2pyspark:latest`.

## 🆕 v6 results (2026-09-28)

v6 is the first model on **Qwen2.5-Coder-7B-Instruct-4bit** (fresh LoRA adapters, rank 16 on the last 8 of 28 blocks, 1,200 updates of 4 samples on the v5 dataset as 4,800 micro-batches of 1; 8 h 19 min, validation loss 0.289 → 0.010; see [Model Training](03-model-training-and-export.md)). Results (`eval/results/v6__*.json`, log `eval/post_training_v6.log`; the chain used `--timeout 600` because the 7B generates ~2x slower):

| Eval set | v3 | v5 | **v6** | Rule |
| :--- | :--- | :--- | :--- | :--- |
| Hold-out (62) | 59 | 56 | **60** (96.8%) | ✅ ≥ 56 (current `latest`) |
| Fixed test set: classic families (97) | 96 | 95 | **96** | ✅ ≥ 94 |
| Fixed test set: v4 families (53) / pipelines (22) | 9 / 0 | 33 / 8 | **36 / 10** | — |
| Fixed test set: all (150) | 105 | 128 | **132** (88.0%) | ✅ ≥ 128 |
| v4 hold-out (25) | 3 | 14 | **16** (64%) | ✅ > 14 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ ≥ 1 |
| Seconds per query: hold-out / fixed test / real | 3.3 / 8.0 / 79 | 4.3 / 11.2 / 76 | 9.9 / 22.5 / 189 | — |

- **Hold-out**: v6 fixes 4 of v5's 6 failures (`holdout041`, `holdout050`, the `UNION ALL` alias case `holdout053` and the `CROSS JOIN` of two `SELECT DISTINCT`s `holdout058`) and adds none. It still fails `holdout015` (`DATEDIFF(CURRENT_DATE(), birth_date)` written as `F.current_date() - "birth_date"`, which subtracts the string literal) and `holdout031` (`UNION` of differently named columns, failing since v3). By difficulty: simple 93%, medium 95%, **complex 100%** (25/25).
- **v4 hold-out**: aggregation 3/4, windows 4/5, joins 3/5, set operations 2/3, reshape 1/2, nested 2/4 and, for the first time, a pipeline (1/2).
- **Fixed test set**: v6 gets 7 queries right that v5 misses (3 pipelines, a CTE chain, a join aggregation, an advanced join and an advanced window) and misses 3 that v5 gets right (an aggregation, an advanced aggregation, a pipeline).
- **Real-world queries**: `real001` still loops (`w1` … `w92` window definitions until the 2,048-token limit, cut off mid-string); `real002`, `real003` and `real004` now parse (v5: 2 of 4) but fail on a column qualifier (`po.rolling_avg_cost_7d`, `ct.device_id`) or a `grouping_id` column that is never created.

**Decision**: v6 meets every condition except the real-world one (0/4, like every model; v5 was promoted by override while missing it and the hold-out condition). Since it beats or ties v5 everywhere, it was promoted on 2026-09-28 (`make promote TAG=v6 PREV=v5`); roll back with `ollama cp sql2pyspark:v5 sql2pyspark:latest`. Trade-off: about 2x slower answers and ~8 GB in Ollama (v5: ~3.5 GB).

## 🆕 v5 results (2026-09-28)

v5 continued training from v4's adapters (1,200 updates, peak LR 5e-5) on a 55% classic / 25% short-feature / 20% pipeline mix generated with the v5 fixes (see [02 - Data Engineering](02-data-engineering-pipeline.md#v5-generator-changes)).

| Eval set | v3 | v4 | v5 | Rule for v5 |
| :--- | :--- | :--- | :--- | :--- |
| Hold-out (62) | 59 | 58 | 56 | ❌ ≥ 59 |
| Fixed test set: classic families (97) | 96 | 84 | **95** | ✅ ≥ 94 |
| Fixed test set: v4 families (53) | 9 | 26 | **33** | — |
| &nbsp;&nbsp;of which pipelines (22) | 0 | 5 | **8** | — |
| Fixed test set: all (150) | 105 | 110 | **128** | ✅ ≥ 105 |
| v4 hold-out (25) | 3 | 9 | **14** | ✅ > 3 |
| Real-world queries (4) | 0 | 0 | 0 | ❌ ≥ 1 |

**Decision**: by the rule v5 misses two conditions (hold-out, real queries). It is nevertheless the best model on the fixed test set and the v4 features, with v3-level accuracy on classic queries, so the owner **promoted it to `latest` on 2026-09-28** as an explicit override; roll back with `ollama cp sql2pyspark:v3 sql2pyspark:latest`.

**v4 hold-out by feature group** (v5 / v4 / v3): aggregation 50% / 25% / 0%, window **100%** / 40% / 40%, join 40% / 20% / 0%, setop 67% / 67% / 33%, reshape 50% / 50% / 0%, nested 50% / 50% / 0%, pipeline 0% / 0% / 0%. New in v5: `GROUPING SETS` over a join (`001`), the epoch `RANGE` frame (`006`), the named `WINDOW` (`007`), the 6-hour sessionization (`009`), `LATERAL ... LIMIT 1` (`011`), the `<=>` join (`013`) and `LATERAL VIEW OUTER EXPLODE` (`020`). Lost versus v4: the `INTERVAL` range join (`010`, now a syntax error) and the struct / map / JSON query (`023`). Still failing: `ROLLUP` + `GROUPING_ID` (`002`, wrong column order), percentiles / `COLLECT_SET` (`004`), correlated `NOT EXISTS` (`012`), the scalar subquery (`014`), the renamed-column `UNION` (`015`), `UNPIVOT` (`019`, wrong rows), `POSEXPLODE` (`021`) and both pipelines (`024` rank without `ORDER BY`, `025` struct-field confusion).

**Hold-out (62)**: v5 keeps v3's three failures (`031`, `050`, `053`) and v4's `058`, and adds two slips on simple expressions: `holdout015` renders `DATEDIFF(...) / 365` as `... / 365.alias(...)` (a Python float), and `holdout041` never assigns `result`.

**Real-world queries**: `real001` still loops on window definitions (`w1` … `w93`) until the token limit, `real002` leaves a parenthesis open in a join condition, `real004` loses a quote in a regex literal; `real003` parses and runs, but names an `UNPIVOT` value `temperature` instead of `TEMPERATURE`. Two replies parse (v4: 1, v3: 0).

**Next levers**: the window-reuse data did not break `real001`'s loop; a larger base model (e.g. 7B) or generation-side controls (a repetition penalty for long outputs) are the next things to try, together with more long pipelines and the remaining hold-out gaps.

## 🆕 v4 results (2026-09-27)

| Eval set | v3 | v4 | |
| :--- | :--- | :--- | :--- |
| Hold-out (62) | 59/62 (95.2%) | 58/62 (93.5%) | ❌ one regression |
| v4 hold-out (25) | 3/25 (12%) | **9/25 (36%)** | ✅ ×3 |
| Real-world queries (4) | 0/4 | 0/4 | ❌ no gain |
| Test sample (150, v4 split) | 105/150 (70.0%) | 110/150 (73.3%) | ⚠️ +5 overall, −12 on classic queries |

**Decision**: v4 misses the promotion rule, so `sql2pyspark:latest` stayed on v3. On the 150-query test sample v4 is 5 queries ahead overall, but only because it gains 17 on the v4 families (26/53 vs. 9/53; long pipelines 5/22 vs. 0/22) while losing 12 on the classic v1–v3 families (84/97 vs. 96/97: 5 syntax errors, 3 runtime errors, 2 wrong column lists, 2 wrong row sets), so v4 is not a safe drop-in replacement either.

**v4 hold-out by feature group** (correct, lenient): aggregation 25% (v3 0%), window 40% (40%), join 20% (0%), setop 67% (33%), reshape 50% (0%), nested 50% (0%), pipeline 0% (0%). v4 newly solves `FILTER (WHERE ...)` / `COUNT_IF` / `NULLIF` (`003`), `DENSE_RANK ... NULLS LAST` + `LAST_VALUE ... IGNORE NULLS` (`005`), the `INTERVAL` range join (`010`), `INTERSECT ALL` (`017`), multi-aggregate `PIVOT` (`018`), the lambda query (`AGGREGATE` / `TRANSFORM` / `FILTER` / `EXISTS` / `ZIP_WITH`, `022`) and the struct / map / `TRY_CAST` / JSON query (`023`), and keeps `QUALIFY` (`008`) and `EXCEPT ALL` (`016`). It loses the named-`WINDOW` query that v3 solved (`007`) and still fails `GROUPING SETS` over a join, `ROLLUP` + `GROUPING_ID`, percentiles / `COLLECT_SET`, the epoch `RANGE` frame, the 6-hour sessionization, `LATERAL ... LIMIT 1`, correlated `NOT EXISTS`, the `<=>` join, the scalar subquery, the renamed-column `UNION`, `UNPIVOT`, both explode queries and both pipelines.

**Hold-out (62)**: v4 keeps v3's three failures (`holdout031` `UNION` of differently named columns, `holdout050` `EXISTS` with three conditions, now an alias error instead of a syntax error, `holdout053` aliases in `UNION ALL` with literals) and adds one: `holdout058` drops `.distinct()` on one side of a CROSS JOIN of two `SELECT DISTINCT` subqueries (180 rows instead of 20). Every v4 reply parses (100% syntax vs. 97%).

**Real-world queries**:
- `real001`: v4 falls into a repetition loop, defining the same window `w1` … `w93` until the 2,048-token limit.
- `real002`: the null-safe join `<=>` is turned into `=` (a Python syntax error).
- `real003`: the reply parses and runs, then fails on a column qualifier (`src.metric_name`); the closest of the four.
- `real004`: a parenthesis is left open in the final chain.

The real queries are 1,200–2,400 tokens and combine ~15 features each; the v4 pipelines teach the individual features, but rarely at that length and density (median ~1,100 tokens). Targets for v5: keep v3's accuracy on the classic families (more training steps, or continue from v3's adapters, since the v4 run saw the same ~4,800 samples as v3 but only ~58% classic ones), more and longer pipeline samples, `<=>`, `QUALIFY`, `UNPIVOT`, `LATERAL`, lambda-heavy nested queries, and the long-standing `UNION` alias gaps.

## 🔁 Repetition-penalty experiment (2026-09-28)

`real001` makes v5 (and v4) repeat window definitions (`w1` … `w93`) until the 2,048-token limit. Ollama already applies `repeat_penalty` 1.1 over the last 64 tokens (`repeat_last_n`), which is shorter than the repeated lines. v5 was re-scored with stronger settings via `evaluate.py --option KEY=VALUE` (results in `eval/results/v5-repeat-penalty/`, outside the report's glob):

| Setting | Hold-out (62) | Real queries (4) | `real001` |
| :--- | :--- | :--- | :--- |
| Default (1.1 over 64 tokens) | **56** | 0 correct, 2 parse | loops: 92 window definitions, cut off |
| `repeat_last_n=2048` (1.1 over 2,048) | 55 | 0 correct, 1 parses | loop gone: 3 windows, complete reply, one syntax error |
| `repeat_penalty=1.2`, `repeat_last_n=256` | 49 | 0 correct, 1 parses | loop gone, parses, fails on a column carried through wrongly |
| `repeat_penalty=1.3`, `repeat_last_n=256` | 41 | 0 correct, 0 parse | — |

(`repeat_last_n=-1`, "whole context", is rejected by Ollama 0.34.4 with HTTP 400.)

Stronger penalties make the model misspell identifiers it has already written (`docotr_id`, `avg_base_fair`): all 7 hold-out queries lost with `repeat_penalty=1.2` are such misspellings. No setting makes a real query correct, so correctness is a model limit, not a decoding one. Adopted: **`repeat_last_n=2048` for SQL longer than 2,000 characters only**, in `app.py` (`generation_options()`); every hold-out query is under 410 characters and the real queries are 4,200+, so short queries behave exactly as before while long ones no longer loop. The `Modelfile` and the evaluation defaults are unchanged.

**v6 (7B)**: with `repeat_last_n=2048` v6 still loops on `real001` (the same 92-window reply, cut off at the token limit; `eval/results/v6-repeat-penalty/`), and the other three real queries fail exactly as with the defaults, so the setting has no effect on v6's real-query results. The app keeps it (harmless on short SQL); the loop's syntax error is flagged by the app's static check, and the Kimi K3 repair fixed v5's, v6's and v7's looping `real001` answers (see below).

A stronger penalty on v6 (`repeat_penalty=1.2`, `repeat_last_n=256`; `eval/results/v6-repeat-penalty/B_*`) stops the `real001` loop (3 window definitions) but the code still fails (undefined names), no real query becomes correct, and the hold-out drops from 60 to **53**, the same pattern as with v5. Decoding settings are therefore not a fix; v7's data targets the loop's trigger instead (see [v7 results](#-v7-results-2026-09-29)).

---

## 🤖 Agent repair loop with Kimi K3 (2026-09-28)

Can a large model fix the model's wrong answers if it may run them? `eval/agent_repair.py` gives **Kimi K3** (Moonshot AI, `kimi-k3` via the OpenAI-compatible API at `api.moonshot.ai`, `reasoning_effort=high`) each real query's SQL, the schemas of the tables it reads and the model's answer, plus one tool, `run_pyspark`, which runs a snippet on the same sample tables as the evaluation and returns the error or the first 20 rows of `result`. The agent never sees the expected output, code calling `.sql(...)` is refused (so it cannot run the original query), and it gets at most 5 runs per query. Its final code is scored like `evaluate.py`.

### v6 repair results (sample tables)
`eval/agent_repair.py --results eval/results/v6__real_queries.json` (results in `eval/results/v6-repair/real_queries.json`; log `eval/agent_repair_v6.log`):

| Query | v6 alone | After repair | Runs | Time | Cost |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `real001` | syntax error (repetition loop `w1`…`w92`) | ✅ correct | 1 | 1176 s | $0.17 |
| `real002` | `AnalysisException` (`po.rolling_avg_cost_7d`) | ✅ correct | 1 | 258 s | $0.16 |
| `real003` | `AnalysisException` (`ct.device_id`) | ❌ runs, 6 rows instead of 4 (`UNPIVOT` NULLs) | 1 | 230 s | $0.16 |
| `real004` | `AnalysisException` (missing `grouping_id`) | ✅ correct | 1 | 138 s | $0.09 |
| **Total** | **0/4** | **3/4** (4/4 run) | **4** | 30.0 min | **$0.59** |

Tokens: 50.8k input (12.8k served from Kimi's prompt cache) and 31.3k output, priced at $3 / $0.30 (cached) / $15 per million. Because 7B generated better-structured code (3 of 4 parsed immediately), Kimi required only **1 tool run per query** (4 total vs. 11 for v5), reducing cost from **$0.90 to $0.59** (−34%).

### v5 repair results (sample tables)
Results in `eval/results/v5-repair/` (log `eval/agent_repair_v5.log`):

| Query | v5 alone | After repair | Runs | Time | Cost |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `real001` | `NameError` (`w93`, repetition loop) | ✅ correct | 2 | 308 s | $0.26 |
| `real002` | syntax error (unclosed `(`) | ✅ correct | 4 | 310 s | $0.27 |
| `real003` | unresolved column `temperature` | ❌ runs, 6 rows instead of 4 | 2 | 162 s | $0.13 |
| `real004` | syntax error (unterminated string) | ✅ correct | 3 | 319 s | $0.24 |
| **Total** | **0/4** | **3/4** (4/4 run) | 11 | 18.4 min | **$0.90** |

Tokens: 148k input (93k served from Kimi's prompt cache) and 47k output.

`real003` misses one semantic detail: Spark SQL's `UNPIVOT` drops rows whose value is NULL by default (`EXCLUDE NULLS`), while `DataFrame.unpivot()` keeps them, so the forward-filled NULL readings add two groups. Adding `.filter(F.col("metric_value").isNotNull())` after the `unpivot` makes Kimi's code match exactly. The agent could not notice, because nothing in the sample output looks wrong without the expected rows.

### The app's setting: no data (`--mode empty`)
In the app there are no sample tables, so the tool runs the code on empty tables whose schemas Kimi declares from the SQL and compares its columns with the SQL's (`kimi_repair.EmptyTableRunner`). Scored on the sample tables as above (`eval/results/v5-repair/real_queries_empty.json`, log `eval/agent_repair_v5_empty.log`):

| Query | After repair (empty tables) | Runs | Time | Cost |
| :--- | :--- | :--- | :--- | :--- |
| `real001` | ✅ correct | 2 | 189 s | $0.15 |
| `real002` | ✅ correct | 1 | 302 s | $0.24 |
| `real003` | ❌ same 6 rows instead of 4 (`UNPIVOT` NULLs) | 1 | 82 s | $0.13 |
| `real004` | ✅ correct | 1 | 388 s | $0.27 |
| **Total** | **3/4** | 5 | 16.2 min | **$0.79** |

Same score without any data: the failures were syntax, names and columns, which empty tables expose, and Kimi reasoned more before its first run (fewer runs, 44k output tokens).

### Repair across models (one run per model and mode)

| Answers repaired | Sample tables | Empty tables (the app) |
| :--- | :--- | :--- |
| v5 (3B) | 3/4, 11 runs, $0.90, 18 min | 3/4, 5 runs, $0.79, 16 min |
| v6 (7B) | 3/4, 4 runs, $0.59, 32 min | **4/4**, 4 runs, $0.70, 19 min |
| v7 (7B) | **4/4**, 5 runs, $0.59, 14 min | 3/4, 5 runs, $0.69, 16 min |

(`eval/results/v{5,6,7}-repair/`, logs `eval/agent_repair_v{5,6,7}*.log`.) `real001`, `real002` and `real004` were fixed in every run; `real003` (the `UNPIVOT` NULL rows) was fixed in 2 of 6 runs, with no pattern by model or mode, so the repair reliably gets 3 of the 4 real queries and sometimes the fourth. The 7B answers needed fewer tool runs than v5's (4-5 vs. 5-11), consistent with fewer structural errors to fix, but with one run per setting that difference is indicative only.

### Repair with explicit rules (2026-09-29, 3 runs per mode)

To handle the `real003` UNPIVOT discrepancy and positional set operations, two general Spark-SQL rules were added to `RULES` in `kimi_repair.py`:
1. **UNPIVOT nulls**: Spark SQL `UNPIVOT` drops rows whose value is NULL unless it says `INCLUDE NULLS`, whereas `DataFrame.unpivot()` keeps them, so the code must filter them out (`.filter(F.col("val").isNotNull())`).
2. **Positional set operations**: SQL set operations (`UNION`, `INTERSECT`, `EXCEPT`) match columns by position, whereas `unionByName()` matches by name; when the branches name columns differently the code must use `.union()`. `static_check()` also flags this in the app via `setop_check()`.

These are general Spark semantics, not query-specific hints, but the `UNPIVOT` rule was written after seeing `real003` fail, so its effect is measured on the same query. v7's real-query answers, 3 runs per mode (`eval/results/v7-repair-rules/run{1,2,3}_{sample,empty}.json`, logs `eval/logs-repair-rules/`):

| Run | Sample tables | Empty tables (the app) |
| :--- | :--- | :--- |
| 1 | 4/4, 4 tool runs, $0.51, 11 min | 4/4, 6 runs, $0.94, 20 min |
| 2 | 3/4 (`real002`: a row difference), 8 runs, $0.73, 14 min | 4/4, 5 runs, $0.76, 16 min |
| 3 | 4/4, 6 runs, $0.63, 14 min | 4/4, 5 runs, $0.66, 15 min |
| **Total** | **11/12** | **12/12** |

`real003` is now fixed in 6 of 6 runs (2 of 6 before the rules); `real002` failed once, the first time in any run. Total cost $4.23 for 24 repairs (~$0.18 each).

**`holdout031` in the app flow**: the static check flags v7's `.unionByName()` for the `city`/`hospital` `UNION`, and the repair (empty tables) turns it into `.union()` in 2 tool runs, 26 s and $0.01; the result matches the SQL (`eval/results/v7-repair-rules/holdout031_empty.json`).

Takeaways: an execution-feedback loop turns long-query failures (loops, syntax errors, wrong names) into correct code at about $0.07–$0.27 and a few minutes per query; with the Spark-default rules it got 23 of 24 repairs of v7's real-query answers right.

---

## 🏃 Running Evaluations

Use `make eval TAG=v9 PREV=v8` (compares `TAG` with `PREV`; the Makefile defaults are `TAG=v9 PREV=v8`), or the CLI directly:

```bash
# Hold-out benchmark for one model tag (results -> eval/results/v5__holdout.json)
python eval/evaluate.py --model sql2pyspark:v5 --set eval/holdout.jsonl

# Fixed test set (150 queries; ~11 s per query on 3B, ~22 s on 7B)
python eval/evaluate.py --model sql2pyspark:v5 --set eval/test_fixed.jsonl

# Re-score stored replies without calling the model (e.g. after changing the scorer)
python eval/evaluate.py --model sql2pyspark:v1 --set eval/holdout.jsonl --out eval/results/v1__holdout.json --reuse-replies

# Real-world queries on their own sample tables
python eval/evaluate.py --model sql2pyspark:v5 --set eval/real_queries.jsonl --tables eval/real_tables.py

# v4 feature hold-out set
python eval/evaluate.py --model sql2pyspark:v5 --set eval/holdout_v4.jsonl

# Evaluating 7B models: increase timeout to 600 s (7B generates ~2x slower than 3B on long queries)
python eval/evaluate.py --model sql2pyspark:v6 --set eval/holdout.jsonl --timeout 600

# Extra Ollama options (e.g. the repetition-penalty experiment)
python eval/evaluate.py --model sql2pyspark:v5 --set eval/real_queries.jsonl --tables eval/real_tables.py --option repeat_last_n=2048 --out /tmp/rp.json

# Agent repair of v5's answers with Kimi K3 (KIMI_API_KEY in .env; --dry-run needs no key)
python eval/agent_repair.py --results eval/results/v5__real_queries.json               # sample tables
python eval/agent_repair.py --results eval/results/v5__real_queries.json --mode empty  # like the app

# Automated v6 post-training evaluation chain
# (waits for training exit, exports v6, scores all 4 sets with --timeout 600, restarts Docker + app)
./eval/post_training_v6.sh

# Rebuild the results table in README.md
python eval/report.py --readme README.md
```

Replies are capped at `--num-predict` tokens (default 2,048; it was a fixed 1,024 before 2026-09-27, which cut off only the real-world query and one looping v1 answer, so earlier scores are unaffected). Set `OLLAMA_URL` to evaluate a model on another Ollama server. Don't run evaluations while training on a 16 GB Mac (GPU memory).

The evaluation also runs in the Docker `tools` container against the host's Ollama:
```bash
docker compose --profile tools run --rm tools python eval/evaluate.py --model sql2pyspark:v5 --set eval/holdout.jsonl
```
Checked on 2026-09-27: the container run of v3 on the hold-out set gave the same 59/62 as the native run, with identical replies for all 62 queries (temperature 0, fixed seed).
