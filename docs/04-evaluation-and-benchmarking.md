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
| `sql2pyspark:v2` | holdout_v4 | 25 | 92% | 32% | **20%** | 32% | **20%** | 6.5 |
| `sql2pyspark:v3` | holdout_v4 | 25 | 88% | 32% | **12%** | 32% | **12%** | 7.1 |
| `sql2pyspark:v4` | holdout_v4 | 25 | 96% | 60% | **36%** | 60% | **36%** | 7.0 |
| `sql2pyspark:v5` | holdout_v4 | 25 | 88% | 68% | **56%** | 68% | **56%** | 8.1 |
| `sql2pyspark:v2` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 55.3 |
| `sql2pyspark:v3` | real_queries | 4 | 0% | 0% | **0%** | 0% | **0%** | 78.9 |
| `sql2pyspark:v4` | real_queries | 4 | 25% | 0% | **0%** | 0% | **0%** | 92.8 |
| `sql2pyspark:v5` | real_queries | 4 | 50% | 0% | **0%** | 0% | **0%** | 75.6 |
| `sql2pyspark:v3` | test_fixed | 150 | 92% | 73% | **69%** | 73% | **70%** | 8.0 |
| `sql2pyspark:v4` | test_fixed | 150 | 90% | 80% | **73%** | 80% | **73%** | 11.6 |
| `sql2pyspark:v5` | test_fixed | 150 | 97% | 88% | **85%** | 89% | **85%** | 11.2 |

Hold-out, correct results (lenient) by difficulty:

| Model | simple (n=15) | medium (n=22) | complex (n=25) |
|---|---|---|---|
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | 100% | 95% | 92% |
| `sql2pyspark:v4` | 100% | 95% | 88% |
| `sql2pyspark:v5` | 93% | 95% | 84% |

v4 hold-out, correct results (lenient) by feature group:

| Model | aggregation (n=4) | window (n=5) | join (n=5) | setop (n=3) | reshape (n=2) | nested (n=4) | pipeline (n=2) |
|---|---|---|---|---|---|---|---|
| `sql2pyspark:v2` | 0% | 40% | 20% | 67% | 0% | 0% | 0% |
| `sql2pyspark:v3` | 0% | 40% | 0% | 33% | 0% | 0% | 0% |
| `sql2pyspark:v4` | 25% | 40% | 20% | 67% | 50% | 50% | 0% |
| `sql2pyspark:v5` | 50% | 100% | 40% | 67% | 50% | 50% | 0% |

### Test split: the fixed test set (`eval/test_fixed.jsonl`)
The generated test split is recomputed for every dataset version, so each version has different test queries. Since v5 the comparison uses a **fixed test set**: the 150-query sample of the v4 test split (seed 0), saved as `eval/test_fixed.jsonl` and removed from later training data (`convert_sql_pyspark.py --exclude`). v3's and v4's stored replies were re-scored on it with identical results, so v3, v4 and v5 are compared on exactly the same queries. Of the 150, 97 come from the classic v1–v3 families and 53 from the v4 families (22 pipelines).

Earlier samples are kept outside the report's glob: `eval/results/v4-split/` (the same 150 queries, v3/v4 files before the rename), `v3-split/` (300 queries of the v3 split: v2 286, v3 284) and `v2-split/` (v1 **8%**, v2 **98%**).

### Promotions
- **v2** beat v1 on hold-out correctness (90% vs. 6%) and was promoted to `sql2pyspark:latest`.
- **v3** beat v2 on the hold-out set (95.2% vs. 90.3%: it fixes 3 of v2's 6 failures with no regressions) and tied it on the same test sample, so it was promoted on 2026-09-26 (`make promote TAG=v3 PREV=v2`). v2 stays available as `sql2pyspark:v2`.
- **v4** (2026-09-27) was **not promoted**; see [v4 results](#-v4-results-2026-09-27). It stays available as `sql2pyspark:v4`.
- **v5** (2026-09-28) was **not promoted** either; see [v5 results](#-v5-results-2026-09-28). It stays available as `sql2pyspark:v5`.

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

**Decision**: v5 misses two conditions, so `sql2pyspark:latest` stays on v3. v5 is nevertheless the best model on the fixed test set and the v4 features, with v3-level accuracy on classic queries.

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

**Decision**: v4 misses the promotion rule, so `sql2pyspark:latest` stays on v3. On the 150-query test sample v4 is 5 queries ahead overall, but only because it gains 17 on the v4 families (26/53 vs. 9/53; long pipelines 5/22 vs. 0/22) while losing 12 on the classic v1–v3 families (84/97 vs. 96/97: 5 syntax errors, 3 runtime errors, 2 wrong column lists, 2 wrong row sets), so v4 is not a safe drop-in replacement either.

**v4 hold-out by feature group** (correct, lenient): aggregation 25% (v3 0%), window 40% (40%), join 20% (0%), setop 67% (33%), reshape 50% (0%), nested 50% (0%), pipeline 0% (0%). v4 newly solves `FILTER (WHERE ...)` / `COUNT_IF` / `NULLIF` (`003`), `DENSE_RANK ... NULLS LAST` + `LAST_VALUE ... IGNORE NULLS` (`005`), the `INTERVAL` range join (`010`), `INTERSECT ALL` (`017`), multi-aggregate `PIVOT` (`018`), the lambda query (`AGGREGATE` / `TRANSFORM` / `FILTER` / `EXISTS` / `ZIP_WITH`, `022`) and the struct / map / `TRY_CAST` / JSON query (`023`), and keeps `QUALIFY` (`008`) and `EXCEPT ALL` (`016`). It loses the named-`WINDOW` query that v3 solved (`007`) and still fails `GROUPING SETS` over a join, `ROLLUP` + `GROUPING_ID`, percentiles / `COLLECT_SET`, the epoch `RANGE` frame, the 6-hour sessionization, `LATERAL ... LIMIT 1`, correlated `NOT EXISTS`, the `<=>` join, the scalar subquery, the renamed-column `UNION`, `UNPIVOT`, both explode queries and both pipelines.

**Hold-out (62)**: v4 keeps v3's three failures (`holdout031` `UNION` of differently named columns, `holdout050` `EXISTS` with three conditions, now an alias error instead of a syntax error, `holdout053` aliases in `UNION ALL` with literals) and adds one: `holdout058` drops `.distinct()` on one side of a CROSS JOIN of two `SELECT DISTINCT` subqueries (180 rows instead of 20). Every v4 reply parses (100% syntax vs. 97%).

**Real-world queries**:
- `real001`: v4 falls into a repetition loop, defining the same window `w1` … `w93` until the 2,048-token limit.
- `real002`: the null-safe join `<=>` is turned into `=` (a Python syntax error).
- `real003`: the reply parses and runs, then fails on a column qualifier (`src.metric_name`); the closest of the four.
- `real004`: a parenthesis is left open in the final chain.

The real queries are 1,200–2,400 tokens and combine ~15 features each; the v4 pipelines teach the individual features, but rarely at that length and density (median ~1,100 tokens). Targets for v5: keep v3's accuracy on the classic families (more training steps, or continue from v3's adapters, since the v4 run saw the same ~4,800 samples as v3 but only ~58% classic ones), more and longer pipeline samples, `<=>`, `QUALIFY`, `UNPIVOT`, `LATERAL`, lambda-heavy nested queries, and the long-standing `UNION` alias gaps.

---

## 🏃 Running Evaluations

Use `make eval TAG=v4 PREV=v3` (compares `TAG` with `PREV`; the Makefile defaults are `TAG=v4 PREV=v3`), or the CLI directly:

```bash
# Hold-out benchmark for one model tag (results -> eval/results/v4__holdout.json)
python eval/evaluate.py --model sql2pyspark:v4 --set eval/holdout.jsonl

# Fixed test set (150 queries; ~11 s per query)
python eval/evaluate.py --model sql2pyspark:v5 --set eval/test_fixed.jsonl

# Re-score stored replies without calling the model (e.g. after changing the scorer)
python eval/evaluate.py --model sql2pyspark:v1 --set eval/holdout.jsonl --out eval/results/v1__holdout.json --reuse-replies

# Real-world queries on their own sample tables
python eval/evaluate.py --model sql2pyspark:v4 --set eval/real_queries.jsonl --tables eval/real_tables.py

# v4 feature hold-out set
python eval/evaluate.py --model sql2pyspark:v4 --set eval/holdout_v4.jsonl

# Rebuild the results table in README.md
python eval/report.py --readme README.md
```

Replies are capped at `--num-predict` tokens (default 2,048; it was a fixed 1,024 before 2026-09-27, which cut off only the real-world query and one looping v1 answer, so earlier scores are unaffected). Set `OLLAMA_URL` to evaluate a model on another Ollama server. Don't run evaluations while training on a 16 GB Mac (GPU memory).

The evaluation also runs in the Docker `tools` container against the host's Ollama:
```bash
docker compose --profile tools run --rm tools python eval/evaluate.py --model sql2pyspark:v3 --set eval/holdout.jsonl
```
Checked on 2026-09-27: the container run of v3 on the hold-out set gave the same 59/62 as the native run, with identical replies for all 62 queries (temperature 0, fixed seed).
