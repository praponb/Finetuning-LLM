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

Generated by [eval/report.py](../eval/report.py) from `eval/results/*.json`:

| Model | Eval Set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v2` | test_meta (v3 split) | 300 | 99% | 99% | **95%** | 99% | **95%** | 6.0 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v3` | test_meta (v3 split) | 300 | 100% | 98% | **95%** | 98% | **95%** | 5.3 |

### Hold-Out Results by Difficulty (Correct, Lenient):
| Model | Simple (n=15) | Medium (n=22) | Complex (n=25) |
| :--- | :--- | :--- | :--- |
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | **100%** | **95%** | **92%** |

### Test split: compare models on the same split
The test split is recomputed for every dataset version, so each version has different test queries. Both `test_meta` rows above use the **same 300-query sample of the v3 split** (seed 0). On it, v2 and v3 are effectively tied: v2 286/300, v3 284/300 (5 queries only v2 gets right, 3 only v3).

The earlier results on the **v2 split** are kept in `eval/results/v2-split/` (outside the report's glob, so they are not in the table): v1 **8%**, v2 **98%** (lenient, 300 queries). They are not comparable with the v3-split rows.

### Promotions
- **v2** beat v1 on hold-out correctness (90% vs. 6%) and was promoted to `sql2pyspark:latest`.
- **v3** beat v2 on the hold-out set (95.2% vs. 90.3%: it fixes 3 of v2's 6 failures with no regressions) and tied it on the same test sample, so it was promoted on 2026-09-26 (`make promote TAG=v3 PREV=v2`). v2 stays available as `sql2pyspark:v2`.

The rule: promote a new model only if it beats the current one on the hold-out set, and check that it doesn't regress on the same test sample.

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

**Shared gap:** all 9 set-operation queries in the test sample share one held-out signature: `UNION` + a literal `source` column + `ORDER BY`. Both models drop the `.distinct()` after `unionByName`, returning ~80 rows instead of ~8 (v2 3/9 correct, v3 1/9). This signature never occurs in training, so it is the main target for a v4 dataset, together with the two hold-out `UNION` failures.

---

## 🏃 Running Evaluations

Use `make eval TAG=v3 PREV=v2` (compares `TAG` with `PREV`; pass both, the Makefile defaults are still `v2`/`v1`), or the CLI directly:

```bash
# Hold-out benchmark for one model tag
python eval/evaluate.py --model sql2pyspark:v3 --set eval/holdout.jsonl

# 300-query sample of the held-out test split
python eval/evaluate.py --model sql2pyspark:v3 --set sql2pyspark-mlx/data/test_meta.jsonl --sample 300

# Re-score stored replies without calling the model (e.g. after changing the scorer)
python eval/evaluate.py --model sql2pyspark:v1 --set eval/holdout.jsonl --out eval/results/v1__holdout.json --reuse-replies

# Rebuild the results table in README.md
python eval/report.py --readme README.md
```

Set `OLLAMA_URL` to evaluate a model on another Ollama server. Don't run evaluations while training on a 16 GB Mac (GPU memory).

The evaluation also runs in the Docker `tools` container against the host's Ollama:
```bash
docker compose --profile tools run --rm tools python eval/evaluate.py --model sql2pyspark:v3 --set eval/holdout.jsonl
```
Checked on 2026-09-27: the container run of v3 on the hold-out set gave the same 59/62 as the native run, with identical replies for all 62 queries (temperature 0, fixed seed).
