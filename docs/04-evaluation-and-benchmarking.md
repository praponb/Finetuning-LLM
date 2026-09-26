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
| `sql2pyspark:v1` | test_meta | 300 | 65% | 0% | **0%** | 18% | **8%** | 4.7 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v2` | test_meta | 300 | 100% | 100% | **98%** | 100% | **98%** | 5.3 |

### Hold-Out Results by Difficulty (Correct, Lenient):
| Model | Simple (n=15) | Medium (n=22) | Complex (n=25) |
| :--- | :--- | :--- | :--- |
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | **93%** | **95%** | **84%** |

v2 met the promotion rule (beat v1 on hold-out correctness) and was promoted to `sql2pyspark:latest`.

### Why v1 fails (hold-out, lenient mode)
Of 62 answers, 4 were correct. The rest broke down as:
- **Runtime errors (38)**: `TypeError` (11, e.g. `'Column' object is not callable`), `AnalysisException` (10, e.g. `.orderBy("doctor_count desc")`, bad `USING` joins), `NameError` (9, e.g. bare `desc`, `col`), `AttributeError` (6, e.g. `GroupedData.withColumn`).
- **Syntax errors (13)**: mostly unbalanced parentheses.
- **Wrong columns (4) or rows (2)**, and 1 answer with no DataFrame.

It also never followed the v2 conventions: all 62 answers assign `df_result`, and 59 import `functions as f`. v1 was trained on ~24 fixed templates over one schema, so it had not learned how to handle new tables or query shapes.

### Where v2 still fails
**Hold-out (6 of 62):**
- 2 × `UNION` / `UNION ALL` of differently named columns (`city` ∪ `hospital`, `full_name` ∪ `passenger_name`): it uses `unionByName`, which fails. The generator only produced set operations over identically named columns.
- 1 × `GROUP BY 1` where column 1 is a `CASE`: it grouped by the raw `YEAR(...)` instead.
- 2 × unbalanced parentheses in long expressions (`SUM(SUM(x)) OVER ()`, `DATEDIFF(...) / 365`).
- 1 × a made-up join type (`"left_seats"`) on an `EXISTS` with three conditions.

**Test split (6 of 300):** 5 wrong-row results and 1 runtime error, spread over `select`, `setop`, `special` and `window` families.

These are the targets for the next dataset iteration (see the README's "Known gaps").

---

## 🏃 Running Evaluations

Use `make eval` (compares `TAG` with `PREV`), or the CLI directly:

```bash
# Hold-out benchmark for one model tag
python eval/evaluate.py --model sql2pyspark:v2 --set eval/holdout.jsonl

# 300-query sample of the held-out test split
python eval/evaluate.py --model sql2pyspark:v2 --set sql2pyspark-mlx/data/test_meta.jsonl --sample 300

# Re-score stored replies without calling the model (e.g. after changing the scorer)
python eval/evaluate.py --model sql2pyspark:v1 --set eval/holdout.jsonl --out eval/results/v1__holdout.json --reuse-replies

# Rebuild the results table in README.md
python eval/report.py --readme README.md
```

Set `OLLAMA_URL` to evaluate a model on another Ollama server. Don't run evaluations while training on a 16 GB Mac (GPU memory).
