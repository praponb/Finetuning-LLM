# 02 - Data Engineering & Synthesis Pipeline

The data engineering pipeline in `sql2pyspark-mlx/` generates, verifies, and packages synthetic SQL-to-PySpark training pairs. It addresses the main challenge of fine-tuning code models: **producing execution-verified, leakage-free training pairs at scale.**

---

## 📂 Pipeline Overview

The pipeline runs in three phases (`make data` runs all three):

1. **Synthesis**: compositional query generation from Python objects (`generate_dataset.py` + `sqlspec.py`) plus curated pairs (`handwritten_pairs.py`).
2. **Ground-truth verification**: local PySpark execution comparing the SQL result with the PySpark result (`verify_pairs.py`).
3. **Leakage-free splitting**: signature-based partitioning and chat formatting (`convert_sql_pyspark.py`).

---

## 1. Table Catalog & Schema Definitions (`schemas.py`)

[schemas.py](../sql2pyspark-mlx/schemas.py) defines the relational domain model for the dataset. It provides base tables and extra complex nested tables:

### A. Training Tables (22 total)

- **17 Base Tables** (used by `generate_dataset.py` and `handwritten_pairs.py`):
  - **E-commerce & retail**: `customers`, `orders`, `order_items`, `products`, `suppliers`, `payments`, `shipments`, `stores`, `inventory`, `store_sales`, `subscriptions`
  - **HR**: `employees`, `departments`
  - **Banking**: `accounts`, `transactions`
  - **Web analytics**: `sessions`, `web_events`
- **5 Extra Complex Tables** (`EXTRA_TABLES`, introducing nested types and custom generators):
  - `carts`: arrays of structs (`items`), coupon arrays (`coupon_codes`), address structs (`ship_to`), string-to-string maps (`attributes`)
  - `app_logs`: service/host categoricals, timestamps, latency, HTTP status codes, raw JSON payload strings (`payload_json`), metadata maps (`context`)
  - `price_tiers`: pricing plans, integer limit arrays (`tier_limits`), double arrays (`tier_prices`, `discounts`), currency
  - `device_readings`: device codes, IoT timestamps, environmental telemetry (temp, humidity, pressure), status bitmasks, firmware strings
  - `promotions`: channel, promotion codes, active date ranges, discount percentages

### B. Hold-Out Tables (6 total, `holdout=True`)

Never used by the generator or the hand-written training pairs; only evaluation sets use them:

- **5 Base Hold-Out Tables**: `patients`, `doctors`, `appointments` (healthcare); `flights`, `bookings` (aviation)
- **1 Extra Nested Hold-Out Table**: `vehicle_trips` (driver IDs, trip timestamps, `ARRAY<STRUCT>` stops with wait times and fares, route struct, telemetry JSON strings, extras map, and fare component arrays)

### C. Column Types, Nested Schemas & Synthetic Data

Every table is defined with typed `Column` descriptors supporting primitive and nested data types:

```python
@dataclass(frozen=True)
class Column:
    name: str
    kind: str              # id, fk, int, double, cat, str, date, ts, bool, array, struct, map, json
    pool: tuple = ()       # allowed values for categorical/string/array columns
    lo: float = 0          # numeric range minimum
    hi: float = 100        # numeric range maximum
    ref: str | None = None # foreign-key target table
    nullable: bool = True
```

`make_dataframes(spark, seed=7)` builds deterministic DataFrames for all 28 tables (`TABLES + EXTRA_TABLES`), registers each as a temp view (`createOrReplaceTempView(table.name)`), and returns them as `df_<table_name>`. Tables can provide custom row generation closures (`table.rows(seed)`) for complex nested distributions.

---

## 2. Dual-Rendering Expression Engine (`sqlspec.py`)

[sqlspec.py](../sql2pyspark-mlx/sqlspec.py) keeps the SQL and PySpark sides consistent. Instead of generating text with regexes or templates, it represents each query as a tree of typed objects.

Every expression inherits from base class `E` and implements:

- `.sql(style)`: renders Spark SQL according to the surface `Style`.
- `.py(ctx)`: renders the equivalent PySpark expression.

```mermaid
classDiagram
    class E {
        +kind: str
        +sql(style) str
        +py(ctx) str
        +arg(ctx) str
        +rhs(ctx) str
        +pyw(ctx) str
    }
    class Col { +Column c; +str q }
    class Lit { +v }
    class Raw { +v }
    class BinOp { +E l; +str op; +E r }
    class Fn { +str sql_name; +str py_name; +list args }
    class Case { +list branches; +E else_ }
    class Cast { +E e; +str to }
    class Agg { +str func; +E e; +bool distinct }
    class WinFn { +str sql_name; +str py_name; +list args; +WinSpec spec }
    class P { predicates }
    E <|-- Col
    E <|-- Lit
    E <|-- Raw
    E <|-- BinOp
    E <|-- Fn
    E <|-- Case
    E <|-- Cast
    E <|-- Agg
    E <|-- WinFn
    E <|-- P
    P <|-- Cmp
    P <|-- InList
    P <|-- Between
    P <|-- Like
    P <|-- IsNull
    P <|-- And
    P <|-- Or
    P <|-- Not
```

Query-level objects are not expressions: `Rel` (a table, CTE or derived table), `Join`, `Semi` (EXISTS / IN / NOT EXISTS / NOT IN, rendered as semi/anti joins in PySpark) and `Sel` (a full `SELECT` with joins, filters, grouping, HAVING, ORDER BY and LIMIT). `WinSpec` holds a window's partition, order and frame.

### Surface Styling Variance (`Style`)

To make the model robust to different SQL writing styles, `Style` randomizes non-semantic variations:

- Upper- vs. lower-case keywords (`SELECT` vs. `select`)
- Explicit vs. implicit aliases (`col AS c` vs. `col c`) and short vs. full table aliases
- `<>` vs. `!=`
- `JOIN` vs. `INNER JOIN`, `LEFT JOIN` vs. `LEFT OUTER JOIN`
- Multi-line vs. single-line layout
- Trailing semicolons, explicit `ASC`

### Preamble Management (`Ctx`)

`Ctx` collects window specifications (`w = Window.partitionBy(...)`) and scalar-subquery values, and places them before the main DataFrame chain; `render_code()` adds the imports.

---

## 3. Compositional Query Generator (`generate_dataset.py`)

[generate_dataset.py](../sql2pyspark-mlx/generate_dataset.py) builds queries from **12 families**:

| Family | SQL Pattern | PySpark Implementation |
| :--- | :--- | :--- |
| `fam_select` | `SELECT cols, exprs FROM t WHERE cond ORDER BY ... LIMIT n`, `SELECT *`, `DISTINCT` | `.filter(...).select(...).orderBy(...).limit(n)` / `.withColumn(...)` |
| `fam_agg` | `SELECT g, AGG(x) FROM t WHERE cond GROUP BY g HAVING cond` (keys referenced by expression, alias or ordinal such as `GROUP BY 1` on a `CASE`), `ROLLUP`/`CUBE` | `.filter(...).groupBy(...).agg(...).filter(...)` |
| `fam_join` | `FROM a [INNER\|LEFT\|RIGHT\|FULL] JOIN b ON ...`, chains, self-joins, composite keys | `df_a.alias("a").join(df_b.alias("b"), cond, "left").select(...)` |
| `fam_join_agg` | Joins followed by `GROUP BY` and aggregates | join chain → `.groupBy(...).agg(...)` |
| `fam_window` | `ROW_NUMBER`/`RANK`/`NTILE`/`LAG`/`LEAD`/running & moving aggregates `OVER (...)` | `w = Window...`, `F.row_number().over(w)` |
| `fam_topn` | Top-N per group via a ranked CTE or derived table | `.withColumn("rn", ...).filter(F.col("rn") <= n)` |
| `fam_cte_agg_join` | `WITH totals AS (SELECT ... GROUP BY fk) SELECT ... FROM parent JOIN totals` | named intermediate DataFrame (e.g. `customer_totals = ...`) joined into the main chain |
| `fam_cte_chain` | `WITH t1 AS (...), t2 AS (... FROM t1) SELECT ... FROM t2` | sequential named DataFrames |
| `fam_derived` | `SELECT ... FROM (SELECT ... GROUP BY ...) sub WHERE ...` | one chain: inner steps followed by the outer filter |
| `fam_subquery` | `IN`, `EXISTS`, `NOT EXISTS`, `NOT IN`, scalar and correlated subqueries | `left_semi` / `left_anti` joins (plus a NULL guard for `NOT IN`), a value computed with `.first()[0]`, or a window aggregate for correlated subqueries |
| `fam_setop` | `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` over one table, two tables with shared column names, or two tables with *differently named* columns | `.unionByName()` / `.union()`, `.distinct()`, `.intersect()`, `.subtract()` |
| `fam_special` | `PIVOT`, `EXPLODE` / `LATERAL VIEW`, `CROSS JOIN`, window over an aggregate | `.groupBy().pivot().agg()`, `F.explode()`, `.crossJoin()`, `.withColumn(... .over(w))` after `.agg()` |

Each sample is tagged with feature tags, for example `where_cmp`, `where_like`, `where_null`, `where_or`, `case`, `fn_date`, `fn_str`, `agg_case`, `having`, `order`, `limit`, `join_left`, `window_rank`, `window_frame`, `cte`, `set_union_all`. The tags drive the split in step 6.

### v3 generator changes

v3 targets the v2 hold-out failures (see [04 - Evaluation](04-evaluation-and-benchmarking.md)):

- **Set operations over differently named columns**: 35% of set operations now pair columns with different names (`city` / `location`). The SQL is written positionally, with an alias on the right side, or with a common alias on both sides. The PySpark uses `.union()` when the names differ and `.unionByName()` when they line up, because SQL matches set-operation columns by position. Feature tags: `set_positional`, `set_alias_right`, `set_alias_both`.
- **`GROUP BY` references**: grouping keys are referenced by expression, by alias or by ordinal (`GROUP BY 1`). Ordinals are used more often when a key is a `CASE` expression (35%), so the model learns to keep the `CASE` as the grouping key.
- **Exact `NOT IN` semantics**: `x NOT IN (subquery)` drops rows where `x` is NULL, unless the subquery returns no rows. The PySpark now computes the excluded values first and adds `.filter(F.col(x).isNotNull() | F.lit(excluded.isEmpty()))` after the anti join, so the pair that failed verification in v2 is now correct.

### v4: stage-based families (`advanced.py`)

v4 adds [advanced.py](../sql2pyspark-mlx/advanced.py), bound into `generate_dataset.FAMILIES`, for the SQL that real production queries use and v1–v3 never generated. A query is a chain of **stages**; each stage turns one relation into the next and renders both a SELECT (a CTE body, or the main query for the last stage) and the matching PySpark chain:

| Stage | SQL | PySpark |
| :--- | :--- | :--- |
| `stg_clean` | `WHERE ts >= TIMESTAMP '...'`, `IS NOT NULL`, derived and `COALESCE`d columns | `.filter(...)`, `F.lit(...).cast("timestamp")`, `.withColumn` / `.select("*", ...)` |
| `stg_dedup` | `ROW_NUMBER()` / `RANK() ... = 1` via `QUALIFY` or a subquery, `NULLS FIRST/LAST` | `.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)` |
| `stg_windows` | 2–5 windows with different specs: `LAG/LEAD` (with default), running `ROWS UNBOUNDED`, `ROWS n PRECEDING AND m PRECEDING`, `RANGE` over `CAST(ts AS BIGINT)`, `DENSE_RANK/RANK/PERCENT_RANK/CUME_DIST`, `NTILE`, `FIRST/LAST_VALUE ... IGNORE NULLS`, partition `STDDEV_SAMP/CORR`, shares; optional named `WINDOW` clause | `Window...rowsBetween/rangeBetween`, `.desc_nulls_last()`, `F.last(..., ignorenulls=True)` |
| `stg_session` | gaps-and-islands: `CASE WHEN LAG(ts) ... IS NULL OR ts > LAG(ts) ... + INTERVAL '30' MINUTE` (or `ts - LAG(ts) > INTERVAL ...`), then `SUM(flag) OVER (... ROWS UNBOUNDED ...)` | `F.expr("INTERVAL 30 MINUTES")`, running `F.sum(...).over(w)` |
| `stg_join` | foreign-key joins; validity-window joins (`d >= p.starts_on AND d < p.ends_on + INTERVAL 7 DAYS`, `BETWEEN`, `DATE_ADD`), `<=>` and `COALESCE` keys | compound join conditions, `.eqNullSafe(...)` |
| `stg_lateral` | `[LEFT] JOIN LATERAL (... ORDER BY ... LIMIT 1) x ON TRUE` | join + `row_number()` per source row, keep `rn == 1` |
| `stg_anti` | `[NOT] EXISTS (SELECT 1 ... WHERE c.fk = s.id AND c.d <= s.d)` | `left_anti` / `left_semi` with a compound condition |
| `stg_setop` | `EXCEPT [ALL\|DISTINCT]`, `INTERSECT [ALL]` on key columns, joined back | `.subtract` / `.exceptAll` / `.intersect` / `.intersectAll` |
| `stg_agg` | `GROUP BY` (expression / alias / ordinal), `ROLLUP`, `CUBE`, `GROUPING SETS`, `GROUPING_ID()` / `GROUPING()`, `AGG(x) FILTER (WHERE ...)`, `COUNT_IF`, `SUM(CASE ...)`, `ROUND(... / NULLIF(...), 2)`, `PERCENTILE_APPROX` (scalar or `ARRAY`), `CORR`, `STDDEV_SAMP`, `BIT_OR/BIT_AND`, `SORT_ARRAY(COLLECT_SET(...))`, `HAVING`, rank over the aggregate | `.groupBy / .rollup / .cube / .groupingSets([[...]], ...)`, `F.when` inside aggregates, `F.nullif`, `F.grouping_id()` |
| `stg_pivot` | `PIVOT (SUM(x) [AS a], COUNT(1) AS n FOR c IN ('V' [AS v], ...))` | `.groupBy(...).pivot(c, [...]).agg(...)` + renames to the SQL column names |
| `stg_unpivot` | `UNPIVOT [INCLUDE NULLS] (val FOR name IN (a AS A, b, ...))` | `.unpivot(ids, values, name, val)` (+ `.filter(val.isNotNull())`) |
| `stg_final` | final projection, `CASE WHEN c RLIKE '^(A\|B)$'`, correlated scalar subquery in SELECT, `ORDER BY ... NULLS LAST`, `LIMIT` | lookup DataFrame + left join, `.rlike(...)`, `.desc_nulls_last()` |
| `stg_explode` | `LATERAL VIEW [OUTER] EXPLODE/POSEXPLODE`, `INLINE`, `EXPLODE(map) AS k, v` | `F.explode_outer`, `F.posexplode(...).alias("pos", "item")`, `F.inline` |
| `stg_nested_select` | struct fields, `m['key']`, `TRY_CAST(m['k'] AS DOUBLE)`, `MAP_KEYS`, `GET_JSON_OBJECT`, `FROM_JSON(...).field`, `TRANSFORM`, `FILTER`, `EXISTS`, `FORALL`, `AGGREGATE`, `ZIP_WITH`, `ELEMENT_AT`, `SLICE`, `(x & 4) != 0`, `SHIFTRIGHT` | `F.col("s.f")`, `F.col("m")["k"]`, `.try_cast(...)`, `F.transform(col, lambda x: ...)`, `F.aggregate(...)`, `.bitwiseAND(...)` |

Families: `fam_adv_window`, `fam_adv_dedup`, `fam_adv_agg`, `fam_adv_join`, `fam_adv_reshape`, `fam_adv_setop`, `fam_adv_session` and `fam_adv_nested` are 1–2-stage queries focused on one feature group; `fam_pipeline` chains 4–8 stages (falling back to another stage when one does not apply) into production-style queries with SQL comments, up to ~3.1k tokens as training samples.

Row-order-dependent windows (`ROW_NUMBER`, `LAG`, `NTILE`, `ROWS` frames, `FIRST/LAST_VALUE`) always order by the relation's unique key as a tie-breaker, and are skipped once a relation has no unique key (after an explode or a multiplying join), so SQL and PySpark produce the same rows. CTE names never shadow a table name. In a sample of 540 pairs, 99.6% passed the Spark check; the rest were `CORR` dividing by zero on constant groups (Spark 4 runs in ANSI mode).

Known weaknesses of the v4 data: some `HAVING agg < 0` filters and an inherited `NOT IN` over every value of a column returned empty results, which made their Spark check weak (the pairs were still correct). Both are fixed in v5.

### v5 generator changes

v5 targets what v4 got wrong on the v4 hold-out set (6 of its 16 misses were wrong column lists) and its regression on classic queries:

1. **Exact projections**: windows, session, dedup, foreign-key-join and LATERAL stages usually end with an explicit column list (`subset_cols()`: the keys plus a random subset) instead of `SELECT *` / `s.*`, and drop helper columns (session flags, previous timestamps, `rn`). Feature tag `explicit_projection`.
2. **Null-safe comparisons**: `<=>` keys in foreign-key joins (`.eqNullSafe(...)`) and `IS [NOT] DISTINCT FROM` filters (`~F.col(c).eqNullSafe(F.lit(v))`).
3. **`stg_join_agg`** (new): join a dimension table and aggregate in one SELECT with `GROUPING SETS` / `ROLLUP` / `CUBE` / plain `GROUP BY` and `GROUPING_ID()` (only with the first three); grouping keys that exist in both tables get distinct aliases.
4. **`UNPIVOT`**: `unpivot(ids=..., values=..., variableColumnName=..., valueColumnName=...)` keyword arguments, an explicit output list or `SELECT *`, optional alias after `UNPIVOT (...)`.
5. **Explode**: aliased tables with qualified `LATERAL VIEW` arguments (`FROM carts t LATERAL VIEW OUTER EXPLODE(t.items) li AS line`), item aliases unlike the field names, `pos + 1 AS line_no`.
6. **More `LATERAL`, `[NOT] EXISTS` and correlated scalar subqueries** (higher stage and family weights).
7. **Denser, longer pipelines**: 5–8 stages (v4: 4–8); windows and joins can carry a `WHERE` in the same SELECT (at most one such optional filter per query, so results don't become empty); 35% of window stages compute 3–4 functions over one shared spec (`shared_spec_items()`), so the PySpark reuses one `Window` variable instead of defining many.
8. **Set operations**: 50% of `fam_setop` queries pair differently named columns (v4: 35%), 55% of those are positional; the literal column (`source`, `kind`, `record_type` or `origin`) goes first or last; `SELECT DISTINCT` cross joins are twice as common.
9. **Weak checks**: `HAVING agg < x` now uses a large threshold, and `NOT IN` never lists every value of a column.

v5 data is generated with `--seed 5` (v4: 0) and 20% pipelines (`fam_pipeline` weight 42); the split drops every pair whose SQL appears in the fixed test set (`convert_sql_pyspark.py --exclude ../eval/test_fixed.jsonl`, 2 pairs). About 36% of pipelines still return an empty result on the 40-row sample tables (stacked dedup / anti-join / set-operation stages); their stages pass the check on thousands of non-empty cases.

### v7 generator changes

v7 targets the remaining persistent gaps from v5 and v6 evaluations (`holdout015` date division slips, `holdout031` unrelated column set operations, and window partition expressions):

1. **DATEDIFF arithmetic**: `derive()` in `generate_dataset.py` adds arithmetic on date differences (`DATEDIFF(CURRENT_DATE(), birth_date) / 365 AS approx_years`, `... / 7 AS weeks`, `ROUND(DATEDIFF(...) / 30.0, 1)`), explicitly teaching PySpark `(F.datediff(...) / 365)` and preventing the model from confusing string literals or calling `.alias(...)` on a Python float. Tag `fn_date_arith`.
2. **Window partitioned by date expression**: `date_part_expr()` generates `PARTITION BY DATE_TRUNC('month', ts)`, `YEAR(d)`, `TO_DATE(ts)` within both `generate_dataset.py` (`make_window()`) and `advanced.py` (`make_window_item()`), teaching window specs that partition on derived expressions. Tag `window_expr_partition`.
3. **Unrelated column set operations**: `unrelated_col_pairs()` in `fam_setop` pairs string columns of different tables with different names and completely unrelated values (e.g. `city` vs. `hospital`, as in `holdout031`). SQL set operations combine them by position, requiring PySpark `.union()` instead of `.unionByName()`. Also adds `WHERE <col> IS NOT NULL` filters on both branches (30% chance). Tags `set_unrelated`, `set_not_null_filter`.

v7 data is generated with `--seed 7` (`make data N=20000 SEED=7 WORKERS=4`).

---

## 4. Curated Pairs (`handwritten_pairs.py`)

[handwritten_pairs.py](../sql2pyspark-mlx/handwritten_pairs.py) adds **105 hand-written pairs** (the v1 curated set, ported to the v2 conventions and schema). They cover features the generator produces rarely or never, for example:

- Modulo arithmetic (`%`) and `POWER()`
- `ORDER BY ... NULLS LAST`
- String functions (`SPLIT`, `CONCAT_WS`, `REGEXP_EXTRACT`, `REPLACE`)
- Date arithmetic (`DATEDIFF()`, `DATE_SUB()`, `ADD_MONTHS()`)
- `CROSS JOIN`, `INTERSECT`/`EXCEPT` across tables, `ROLLUP`, `CUBE`, `PIVOT`

All 105 are execution-verified and always go to the training split.

---

## 5. Spark Dual-Execution Verifier (`verify_pairs.py`)

[verify_pairs.py](../sql2pyspark-mlx/verify_pairs.py) checks every pair using a pool of worker processes (`multiprocessing`, spawn context), each with its own local `SparkSession`.

```text
                    ┌───────────────────────────────┐
                    │ raw_pairs/*.sql & *.py        │
                    └───────────────┬───────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │ Worker Pool (Checker Instance)│
                    └───────┬───────────────┬───────┘
                            │               │
             spark.sql(sql) │               │ exec(py_code)
                            ▼               ▼
                    ┌──────────────┐ ┌──────────────┐
                    │  SQL Output  │ │PySpark Output│
                    │  DataFrame   │ │  DataFrame   │
                    └───────┬──────┘ └──────┬───────┘
                            │               │
                            └───────┬───────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │      compare() Verification   │
                    │ 1. Column Names Match?        │
                    │ 2. Multiset Row Values Match? │
                    │ 3. ORDER BY Keys Preserved?   │
                    └───────────────┬───────────────┘
                                    │
                       Pass ────────┴──────── Fail
                        │                      │
                        ▼                      ▼
                 verified.jsonl        verify_failures.jsonl
```

### Verification Rules

1. **Column matching**: names and order must be identical (`s_cols == p_cols`).
2. **Multiset equivalence**: rows are normalized (numbers rounded to 4 decimals, dates ISO-formatted, NaN standardized) and compared with `Counter(map(repr, rows))`, so row order doesn't matter.
3. **Ordering**: if the query has `ORDER BY`, the sequence of the ORDER BY columns must match (rows that tie may come out in either order).

### Verification Run Results (`raw_pairs/verify_report.json`)

- **v7**: 20,066 of 20,105 ok, 20,065 after removing 1 fixed-test duplicate (`sql2pyspark-mlx/data_v7.log`, ~28 min; 2,800 return an empty result)
- **v5**: 20,073 of 20,105 ok (`sql2pyspark-mlx/data_v5.log`, ~21 min, 99.84% pass rate)
- **v4**: 20,068 of 20,105 ok (`data_v4.log`, in `runs/v4/`)
- **v3**: 15,105 of 15,105 ok (561 s)

The failing pairs are dropped. In v5, 2,761 passing pairs (14%; v4: 11%) return an empty result, which makes their check weaker; most are long pipelines.

---

## 6. Leakage-Free Dataset Splitting (`convert_sql_pyspark.py`)

[convert_sql_pyspark.py](../sql2pyspark-mlx/convert_sql_pyspark.py) turns `verified.jsonl` into the JSONL chat datasets.

### Signature-Based Split

The split is done by **feature signature**:
$$\text{Signature} = \text{family} \;\|\; \text{sorted}(\text{features})$$

1. Pairs are grouped by signature.
2. Whole signatures are assigned to `valid` (10%) and `test` (5%).
3. A signature is only held out if it holds at most 2% of the pairs and every one of its features keeps at least `min_train_per_feature=30` training examples.
4. All 105 hand-written pairs always go to `train`.

### Final Split Statistics

- **v7 (`data/split_stats.json`)**:
  - **Verified pairs**: 20,065 (minus 1 excluded matching fixed test set)
  - **Training set (`train.jsonl`)**: 16,923 pairs (9,215 signatures)
  - **Validation set (`valid.jsonl`)**: 2,095 pairs (1,198 signatures)
  - **Test set (`test.jsonl`)**: 1,047 pairs (590 signatures)
  - **Signatures shared between train and valid/test**: **0**
  - **Features missing from train**: **0**
- **v5**: verified 20,071; train 16,927 (55.1% classic, 24.5% short v4, 20.4% pipelines) / valid 2,096 / test 1,048
- **v4**: train 16,924 / valid 2,096 / test 1,048 (the `runs/v4/data/` backup was removed on 2026-09-30; still in the git history)

Pipelines have nearly unique signatures, so they make up a larger share of valid/test than of train. Because the split is recomputed for every dataset version, the v2, v3 and v4 test splits contain different queries; compare two models on the same split (see [04 - Evaluation](04-evaluation-and-benchmarking.md)).

`valid_meta.jsonl` / `test_meta.jsonl` keep the SQL, reference code and ORDER BY columns for execution-based evaluation.

### Chat Template Structure

Each record uses the MLX chat format and the same prompt the Ollama runtime and `app.py` use:

```json
{
  "messages": [
    {
      "role": "system",
      "content": "You are an expert data engineer specializing in SQL and Apache Spark. Convert the provided SQL query into clean, idiomatic PySpark DataFrame code."
    },
    {
      "role": "user",
      "content": "Convert SQL to PySpark:\n```sql\nSELECT ...\n```"
    },
    {
      "role": "assistant",
      "content": "```python\nfrom pyspark.sql import functions as F\n\nresult = df_...\n```"
    }
  ]
}
```

v4 training samples are 411 tokens long on average (v3: ~230) and at most 3,119 tokens, under `max_seq_length: 3200`; the answer (the part that is trained) averages 211 tokens.

---

## 7. The complex_v8 dataset (v8)

`sql2pyspark-mlx/complex_v8/` is a **standalone** second pipeline (2026-09-29), built so the 10,000 new pairs needed no change to the v1–v7 generator. It only imports `compare()` / `rows_of()` from `verify_pairs.py` and `split()` / `format_sample()` from `convert_sql_pyspark.py`. See [complex_v8/README.md](../sql2pyspark-mlx/complex_v8/README.md).

| File | Role |
| --- | --- |
| `catalog.py` | 20 **new tables** in 6 domains never used before: music (`artists`, `tracks`, `listeners`, `plays`), insurance (`policyholders`, `adjusters`, `policies`, `claims`), logistics (`warehouses`, `skus`, `stock_moves`), energy (`sites`, `meter_readings`, `tariff_windows`), education (`students`, `courses`, `enrollments`), support (`support_agents`, `tickets`, `ticket_events`); clustered timestamps (bursts of minutes, days between bursts) so session logic has something to find |
| `generate.py` | stage-based generator: every stage emits a SQL CTE and the matching PySpark block together; filter thresholds come from the real sample values, so most filters keep rows |
| `verify.py` | Spark execution check with the same rules as `verify_pairs.py` |
| `build.py` | drops empty-result pairs and pairs over 3,000 tokens, excludes eval-set SQL, splits by feature signature, writes `data/` and `data_mix/` |
| `eval_tables.py` | the catalog for `eval/evaluate.py --tables` (a wrapper: `evaluate.py` loads the file without registering it in `sys.modules`, which breaks `@dataclass` on Python 3.14) |

**Families** (share of the 10,000 training pairs):

| Family | Pairs | Content |
| --- | --- | --- |
| `cx_pipeline` | 2,786 | 3–7 stages: joins (incl. grandparent and multi-joins), EXISTS / NOT EXISTS / IN subqueries → `left_semi` / `left_anti`, correlated scalar subqueries → pre-aggregated LEFT JOIN + COALESCE, dedup with ROW_NUMBER, compare-to-group-average, LATERAL VIEW [OUTER] [POS]EXPLODE, aggregation with FILTER / CASE / HAVING / PERCENTILE_APPROX / COLLECT_SET |
| `cx_window_heavy` | 1,690 | 8–16 window functions over 3–6 window specs (named `WINDOW` clauses, ROWS frames, IGNORE NULLS, NTH_VALUE, NTILE, PERCENT_RANK, share-of-total with NULLIF, windows partitioned by date expressions), often followed by a second window stage |
| `cx_setop` | 1,355 | UNION ALL / UNION / INTERSECT [ALL] / EXCEPT [ALL] / MINUS; **positional unions of differently named columns → `.union()`**, same names → `.unionByName()`; literal source columns; 3-way unions; union-then-aggregate |
| `cx_grouping` | 1,043 | ROLLUP / CUBE / GROUPING SETS (`.groupingSets`), GROUPING_ID / GROUPING, `COALESCE(key, 'ALL')` labels |
| `cx_reshape` | 944 | PIVOT (one or two aggregates) and UNPIVOT (EXCLUDE / INCLUDE NULLS, `col AS label` aliases) |
| `cx_sessionize` | 861 | gaps and islands: LAG → new-session flag (`INTERVAL` or `UNIX_TIMESTAMP` gap) → running SUM → per-session metrics |
| `cx_interval` | 730 | range joins (claims inside policy coverage, readings inside tariff windows, events while a ticket is open) and time-window self-joins |
| `cx_full_outer` | 591 | two aggregates FULL OUTER JOINed on a shared key with COALESCE |

**Build (seed 8)**: 13,000 generated → **12,999 match in Spark** (1 SQL error) → 1,999 empty-result and 175 over-length pairs dropped, 0 eval overlaps → **10,000 train / 500 valid / 300 test**, no empty results, 0 signatures shared between train and valid/test. Samples average **1,163 tokens** (p95 2,421, max 2,999), about 2.4× the v7 data (486). `data_mix/` adds 7,000 random v7 training pairs (17,000 train, ~881 tokens on average) and 500 v7 validation pairs, so a model continued from v7 keeps its classic-query accuracy (v4 lost 12 classic queries when classic samples fell to ~58% of its mix).

```bash
cd sql2pyspark-mlx/complex_v8
../.venv/bin/python generate.py --n 13000 --seed 8 --out raw_pairs     # ~1 min
../.venv/bin/python verify.py --src raw_pairs --workers 5 2>/dev/null   # ~33 min
../.venv/bin/python build.py                                            # split -> data/ + data_mix/
```

---

## 8. The complex_v9 dataset (v9) & "Mirror" Window Style

`sql2pyspark-mlx/complex_v9/` is the training dataset for **v9**. It keeps the SQL, tables, and train/valid/test split structure of [complex_v8](../sql2pyspark-mlx/complex_v8/README.md), but transforms the PySpark representation of queries that use window functions. See [complex_v9/README.md](../sql2pyspark-mlx/complex_v9/README.md).

### The Problem in v8

v8 scored only 1/15 on `cx_window_heavy` on the complex_v8 test set despite having 1,690 training pairs of that family. In the v8 target code ("hoist" style), **every** window specification was defined as a variable at the top of the script. This forced the model to plan 8–16 near-identical lines before generating any DataFrame transformations, frequently triggering token-exhaustion loops (`w1` ... `w93` as also seen in `real001`).

### The "Mirror" Style

The PySpark code now mirrors the SQL structure directly:

| SQL | PySpark (v8, "hoist") | PySpark (v9, "mirror") |
| :--- | :--- | :--- |
| `RANK() OVER (PARTITION BY a ORDER BY b DESC)` | `w_a_by_b = Window...` at top, `.over(w_a_by_b)` | `F.rank().over(Window.partitionBy("a").orderBy(F.col("b").desc()))` |
| `SUM(x) OVER w_a` + `WINDOW w_a AS (PARTITION BY a)` | `w_a = Window...` at top, `.over(w_a)` | `w_a = Window.partitionBy("a")` right before stage, `.over(w_a)` |

`generate.py --window-style mirror` renders this style (the default `hoist` reproduces complex_v8 byte for byte); SQL queries are identical in both styles.

### Dataset Contents & Re-Verification

- **`data/train.jsonl`**: 11,989 pairs (9,999 complex_v8 training pairs re-rendered in mirror style + 1,990 extra `cx_window_heavy` pairs from seed 9).
- **`data/valid.jsonl` (500) & `data/test.jsonl` (300)**: re-rendered complex_v8 validation and test sets.
- **`data_mix/`**: 13,251 training pairs (every complex pair with a window [7,251] + 2,000 complex pairs without windows + 4,000 v7 pairs to retain classic accuracy) and 1,000 validation pairs.
- **Spark verification**: all 15,500 pairs were re-executed in Spark in the mirror style: **15,499 match** (the single failure is the identical SQL error from complex_v8).
- **Leakage check**: the extra pairs have non-empty results and zero signature overlap with valid/test.

### Rebuilding complex_v9

```bash
cd sql2pyspark-mlx/complex_v9
../.venv/bin/python ../complex_v8/generate.py --n 13000 --seed 8 --out raw_pairs --window-style mirror
../.venv/bin/python ../complex_v8/generate.py --n 2500 --seed 9 --out raw_extra --window-style mirror \
    --families window_heavy --prefix wx
../.venv/bin/python ../complex_v8/verify.py --src raw_pairs --workers 5 2>/dev/null    # ~26 min
../.venv/bin/python ../complex_v8/verify.py --src raw_extra --workers 5 2>/dev/null    # ~9 min
../.venv/bin/python build.py                                                           # -> data/ + data_mix/
```

---

## 9. The classic_v10 dataset (v10): classic pairs in mirror style + bare `IN` / `NOT IN`

`sql2pyspark-mlx/classic_v10/` is the training dataset for **v10**, aimed at two failures v9 still had on the classic tables. See [classic_v10/README.md](../sql2pyspark-mlx/classic_v10/README.md).

### The problems in v9

- **`real001` loops.** v9 learned the mirror window style only on the complex_v8 tables. The classic v1–v7 targets still hoisted every window spec (`w`, `w2`, … up to `w21`; 6,829 of the 16,923 v7 training pairs), and v9 repeated exactly that block on the real queries (`w70`, `w71`, …).
- **Bare `IN` / `NOT IN`.** Every classic IN-subquery aliased the outer table in the SQL (`FROM orders o WHERE o.customer_id IN (...)`), and `id NOT IN (SELECT fk FROM child)` never appeared. On the bare form (`holdout024`, `holdout052`) v8/v9 aliased the DataFrames but wrote an unqualified `F.col("id")` in the join condition (`AMBIGUOUS_REFERENCE`).

### What is in it

| File | Content |
| :--- | :--- |
| `mirror.py` | `mirror_code(sql, code)` rewrites hoisted window code: every `name = Window...` (and derived `w2 = w.rowsBetween(...)`, resolved first) is substituted where it is used; a variable stays only when the SQL names the window (`WINDOW w AS (...)`). Returns `None` for multi-line definitions; the result is checked with `ast.parse`. |
| `inlist.py` | 900 targeted pairs: `parent.id [NOT] IN (SELECT child.fk ...)` and `child.fk [NOT] IN (SELECT parent.id ...)` (50% anti), no SQL aliases. The PySpark side aliases both DataFrames and qualifies the join columns (`df_a.alias("a").join(df_b.filter(...).alias("b"), F.col("a.id") == F.col("b.fk"), "left_semi" / "left_anti")`); `NOT IN` over a nullable column gets an explicit `IS NOT NULL` filter. |
| `build.py` | `prepare` (mirror `../raw_pairs/verified.jsonl` → `raw_pairs/`, only changed pairs) and `split` (`data/` with the v7 split by SQL + `data_mix/`) |

- **Spark verification**: 7,857 of the 7,858 rewritten classic pairs still match their SQL (the failure is dropped); 900 / 900 `IN` / `NOT IN` pairs match (867 with a non-empty result are used: 817 train, 50 held out in `data/inlist_valid_meta.jsonl`).
- **`data/`** keeps the v7 train / valid / test membership by SQL, so `../data/test.jsonl` stays held out and `test_fixed` stays excluded.
- **`data_mix/`** (what v10 trains on): 12,463 pairs = 6,646 classic training pairs whose window code changed + 817 `IN` / `NOT IN` + 2,000 classic pairs without windows + 3,000 complex_v9 pairs (already mirror style); validation 850 = 500 classic + 50 `IN` / `NOT IN` + 300 complex. Mean ~793 tokens; 3 samples exceed `max_seq_length` 3,200 and are truncated.

### Rebuilding classic_v10

```bash
cd sql2pyspark-mlx/classic_v10
../.venv/bin/python build.py prepare                                   # ../raw_pairs/verified.jsonl -> raw_pairs/
../.venv/bin/python inlist.py --n 900 --seed 10 --out raw_inlist
../.venv/bin/python ../verify_pairs.py --src raw_pairs --workers 5 2>/dev/null     # ~15 min
../.venv/bin/python ../verify_pairs.py --src raw_inlist --workers 5 2>/dev/null    # ~1.5 min
../.venv/bin/python build.py split
```

### Opt-in: generic union aliases in complex_v8

`complex_v8/generate.py --fix-union-aliases` stops the set-operation families from renaming a key column to a count-like alias (`site_id AS qty`): branch columns are grouped by shape (keys vs. integers) and keys get key-like generic names (`entity_id`, `ref_id`, `key_id`). The default output is byte-identical to complex_v8; with the flag, 2,999 of 3,000 set-operation pairs verify and none has a key → count alias. Not used by any trained model yet.

---

## 10. The targeted_v11 dataset (v11): correlated subqueries with cross conditions, set operations, and LATERAL VIEW filters

`sql2pyspark-mlx/targeted_v11/` is the training dataset for **v11**, designed to eliminate regressions observed in v10 on the hold-out evaluation suite (`holdout_v4`). See [targeted_v11/README.md](../sql2pyspark-mlx/targeted_v11/README.md).

### The Problems in v10

1. **`holdout_v4_012` Regression**: When evaluating correlated `NOT EXISTS` queries whose subquery compares an inner column with an outer column (e.g., `a.appt_date >= p.birth_date`), v10 placed that cross comparison in the inner `.filter()`, where the outer alias `p` was not defined, causing a PySpark runtime `AnalysisException`. Classic datasets only correlated on key equality (`o.customer_id = c.id`), and `classic_v10`'s bare `IN` / `NOT IN` pairs inadvertently overfitted the rule "subquery conditions go in the inner filter".
2. **`holdout_v4_017`**: A projection placed *after* `INTERSECT ALL` instead of selecting columns *before* the set operation.
3. **`holdout_v4_021`**: A `WHERE` condition on an exploded array value applied *before* the explode rather than exploding first and filtering after.

### The Canonical PySpark Rule

> **Subquery Condition Placement Rule:**
> Every condition that references the outer query (cross condition or outer-only condition) **must go into the join condition** (`left_semi` / `left_anti`).
> Only conditions that reference **exclusively inner columns** may go into the inner `.filter()`.

```python
# SQL:
# SELECT c.id, c.name FROM customers c
# WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'PAID'
#                   AND o.order_date >= c.signup_date)

# PySpark (targeted_v11 convention):
result = (
    df_customers.alias("c")
    .join(
        df_orders.filter(F.col("status") == "PAID").alias("o"),
        (F.col("o.customer_id") == F.col("c.id"))
        & (F.col("o.order_date") >= F.col("c.signup_date")),
        "left_anti",
    )
    .select(F.col("c.id"), F.col("c.name"))
)
```

### Dataset Structure & 6 Generator Families

| Family (id prefix) | Train Count | Description & Target Pattern |
| :--- | :--- | :--- |
| `exists_corr` (`exc`) | 4,000 | `[NOT] EXISTS` with 1–2 cross conditions: inequality comparisons, `DATE_ADD`, `DATE_SUB`, `DATEDIFF`, `BETWEEN`, arithmetic (`i.x > o.y * 1.5`), `<>`. Cross and outer-only conditions placed in join condition. Relations: parent → child, child → parent, siblings sharing foreign key, self-joins. |
| `in_corr` (`inc`) | 1,000 | Correlated `x [NOT] IN (SELECT i.k FROM ... WHERE <outer reference>)`. `NOT IN` over nullable columns gets explicit `IS NOT NULL` guards. |
| `exists_multi` (`exm`) | 1,500 | Two subquery predicates in one `WHERE` (`EXISTS` + `NOT EXISTS`, double `EXISTS`, `EXISTS` + `IN`) chained into sequential semi / anti joins. |
| `exists_agg` (`exa`) | 1,500 | Correlated subquery followed by `GROUP BY` (+ `HAVING`, `ORDER BY`), written inline or with filtered rows in a CTE. |
| `setop_proj` (`sop`) | 1,000 | `INTERSECT [ALL]`, `EXCEPT [ALL \| DISTINCT]`, `MINUS`, `UNION` over filtered branches: each branch is filtered and **selected before** the set operation. |
| `lateral_filter` (`lvf`) | 1,000 | `LATERAL VIEW [OUTER] [POS]EXPLODE` over nested arrays/structs (`carts.items`, `carts.coupon_codes`, `products.tags`, `price_tiers`) with `WHERE` on exploded values: **explode first, then filter**. |

### Verification & Splits

- **Spark verification**: 14,700 / 14,700 raw pairs match execution results (`raw_v11/verify_report.json`). 2,661 pairs with empty sample results were dropped.
- **`data/`**: 10,000 `train.jsonl`, 300 `valid.jsonl`, 300 `heldout_meta.jsonl` (50 held-out pairs per family evaluated via `eval/evaluate.py --set`).
- **`data_mix/`**: 15,000 training pairs (10,000 new targeted pairs + 5,000 sampled from `classic_v10/data_mix` covering mirrored classic, bare `IN`/`NOT IN`, and complex_v9). Validation: 600 pairs (300 new + 300 v10). Mean ~1,430 characters per sample.
- **Zero Evaluation Leakage**: Uses only training tables (`train_tables()` + `extra_train_tables()`; `EXTRA_TABLES` also holds the hold-out `vehicle_trips`). The hold-out domain tables (`patients`, `doctors`, `appointments`, `flights`, `bookings`, `vehicle_trips`) never appear in training.

### Rebuilding targeted_v11

```bash
cd sql2pyspark-mlx/targeted_v11
../.venv/bin/python generate.py --seed 11 --out raw_v11                       # 14,700 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v11 --workers 5 2>/dev/null   # ~11 min
../.venv/bin/python build.py                                                 # -> data/ + data_mix/
```

**Result (v11, 2026-10-02)**: v11-q4 scores 283/300 on the held-out targeted pairs (v10-q4 83, v9-q4 130) and fixes `holdout_v4_012` / `017` / `021`. The pairs write one `.filter()` per condition and contain no `OR`; v11 then split a `WHERE a OR b` into two `.filter()` calls on 2 fixed-test queries, so the next version of these generators should add `OR` / mixed `AND`-`OR` conditions. See [Evaluation](04-evaluation-and-benchmarking.md#-v11-results-2026-10-02).

---

## 11. The targeted_v12 dataset (v12): OR / mixed AND-OR conditions

`sql2pyspark-mlx/targeted_v12/` is the training dataset for **v12** ([targeted_v12/README.md](../sql2pyspark-mlx/targeted_v12/README.md)). Its pairs come from `targeted_v11/generate.py --counts v12 --or-rate 0.5` (the default options still reproduce targeted_v11 byte for byte):

- **`or_where`** (2,000 train): one table, `child JOIN parent` or `GROUP BY`, with a `WHERE` that mixes `OR` / `AND` / `NOT`: `a OR b`, `(a AND b) OR c`, `a AND b OR c`, `a OR b AND c` (AND binds tighter: `(pa) | ((pb) & (pc))`), `a AND (b OR c)`, `NOT (a OR b)` (`~((pa) | (pb))`), `a OR b OR c` → **one `.filter()`** with `|` / `&` / `~`.
- **The six targeted_v11 families** (2,700 train): half of their filters become an `OR` of two conditions; some subquery cross conditions are OR'ed with an inner condition (`(o.amount > s.sqft OR o.status IN (...))`), which references the outer query and therefore stays in the **join condition**.
- **Verification & split**: 6,950 / 6,950 raw pairs match in Spark; 597 empty results dropped; nothing shared with an eval set or targeted_v11; 4,700 train, 85 validation, 250 held out (`data/heldout_meta.jsonl`).
- **`data_mix/`** (12,976): the 4,700 new pairs + all 976 classic v7 training pairs with `OR` + 2,800 other classic + 2,500 complex_v9 + 2,000 targeted_v11 (4,982 samples with `OR`).

```bash
cd sql2pyspark-mlx/targeted_v12
../.venv/bin/python ../targeted_v11/generate.py --seed 12 --counts v12 --or-rate 0.5 --out raw_v12   # 6,950 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v12 --workers 5 2>/dev/null                         # ~5 min
../.venv/bin/python build.py
```

**Result (v12, 2026-10-03)**: the fixed-test `OR` queries are back to 11/12 (v11 9) and the held-out `or_where` pairs to 97/100 (v11 79), with the subquery gains kept (287/300, 230/250). See [Evaluation](04-evaluation-and-benchmarking.md#-v12-results-2026-10-03).

---

## 12. The targeted_v13 dataset (v13): uncorrelated scalar subqueries — a lesson in counter-examples

`sql2pyspark-mlx/targeted_v13/` ([README](../sql2pyspark-mlx/targeted_v13/README.md)) targets v12's last two classic misses, `x >= (SELECT MAX(x) FROM t WHERE ...)`: 2,000 `scalar_where` pairs from `targeted_v11/generate.py --seed 13 --counts v13` (comparisons, `F.lit(v) * 1.5` arithmetic so an empty subquery stays NULL, `BETWEEN` two scalars, `SELECT`-list items, `HAVING`, joins), written as `v = df.filter(...).agg(F.max("x")).first()[0]` then `.filter(F.col("x") >= v)`. 2,600 / 2,600 raw pairs verified; 150 held out. Mix 11,500 = 2,000 new + 4,000 complex_v9 + 2,000 targeted_v12 + 1,500 targeted_v11 + 2,000 classic.

**Result**: v13 learned the form (150/150 held out) but applied it to *correlated* scalar subqueries too (`... WHERE t2.k = t.k`, which need a window or a grouped join): the fixed-test `subquery` family fell from 15/17 to 6/17, and the hold-out from 59 to 52. Like v10's `IN` / `NOT IN` pairs, a narrow targeted family without contrasting pairs over-generalizes; a retry needs correlated counter-examples in the same family and a larger classic replay. v13 is not used.

---

## 13. The targeted_v14 dataset (v14): correlated and uncorrelated scalar subqueries side by side

`sql2pyspark-mlx/targeted_v14/` ([README](../sql2pyspark-mlx/targeted_v14/README.md)) is that retry. [`generate.py`](../sql2pyspark-mlx/targeted_v14/generate.py) (reusing the targeted_v11 helpers and the v13 builder) makes 4 families on the 17 training tables, where the deciding detail is whether the inner `WHERE` refers to the outer alias:

| Family | Train | PySpark target |
| --- | --- | --- |
| `scalar_uncorr` | 2,500 | `v = df.filter(...).agg(...).first()[0]` — the v13 shapes, plus aliased same-table forms whose inner `WHERE` compares the would-be key with a literal (`b.city = 'Paris'`) |
| `scalar_corr_win` | 3,000 | same-table correlation on a non-NULL key (`b.city = a.city`) -> `.withColumn("avg_x_by_city", F.avg("x").over(Window.partitionBy("city")))` *before* any outer filter; with an inner filter -> filtered `groupBy("city")` + join on `"city"` |
| `scalar_corr_join` | 2,500 | a parent row against an aggregate of its child rows -> `groupBy(fk)` + join on `fk = p.id`; `COUNT` -> left join + `F.coalesce(..., F.lit(0))` |
| `scalar_mixed` | 2,000 | a correlated and an uncorrelated scalar subquery in one query |

13,500 raw pairs, 13,499 verified (one divides by zero in Spark SQL), 1,172 empty dropped; 50 validation and 50 held out per family. Mix 25,008 = 10,000 new + all 708 classic scalar-subquery pairs + 5,000 classic + 500 classic `OR` + 4,500 complex_v9 + 2,500 targeted_v12 + 1,800 targeted_v11; validation = 200 new + the 36 classic scalar-subquery validation pairs + 150 v12 pairs.

**Result (v14, 2026-10-07)**: the contrast worked — classic `subquery` family 17/17 (v12 15, v13 6), 196/200 held-out pairs — but v14 copies training-table names into unseen schemas (`apppt_date`, `num_stores` for `num_stops`, `ss.restock`): hold-out 58/62, v4 hold-out 18/25. The iter-2,400 checkpoint shows the same slips, so the cause is the data (10,000 more pairs on the same 17 tables), not the training length. v14 is not promoted. See [Evaluation](04-evaluation-and-benchmarking.md#-v14-results-2026-10-07--not-promoted).

---

## 14. The schema_v15 dataset (v15): the same forms on synthetic schemas with unseen names

`sql2pyspark-mlx/schema_v15/` ([README](../sql2pyspark-mlx/schema_v15/README.md)) answers the name drift of v13 / v14 (training names copied into unseen schemas). All data so far used 42 tables with 191 column names; [`synth_catalog.py`](../sql2pyspark-mlx/schema_v15/synth_catalog.py) defines **35 domains, 221 tables, 759 column names** (4–7 tables per domain linked by foreign keys, one nested table per domain with an `ARRAY<STRUCT<...>>` and a `STRUCT` column, `dim_` / `fact_` / `tbl_` prefixes, per-table abbreviations such as `cnt` / `count`, fk columns named `_id` / `_ref` / `_no`, near neighbours of training names such as `store_kiosks`, `restock_qty`, `revenue_share`). No identifier of the hold-out or real-world query tables is used, and no table repeats a training table name (checked at import).

[`patch.py`](../sql2pyspark-mlx/schema_v15/patch.py) installs the catalog into `schemas` before `generate_dataset.py`, `advanced.py` and the targeted generators are imported, so they run unchanged on it (the few branches that name a classic table are skipped; `targeted_v11/generate.py` now tolerates a catalog without `employees`). [`generate.py`](../sql2pyspark-mlx/schema_v15/generate.py) makes 5 groups — classic 4,000, advanced (flat) 1,000, nested 1,000 ([`nested_builder.py`](../sql2pyspark-mlx/schema_v15/nested_builder.py): lambdas over an array of structs, struct fields, `LATERAL VIEW EXPLODE`), subquery / `OR` 2,000, scalar subquery 2,000 — and rewrites window code to the mirror style. [`verify.py`](../sql2pyspark-mlx/schema_v15/verify.py) runs `verify_pairs.py` with the catalog installed in every spawned worker: 13,983 / 14,000 raw pairs pass. The 5 domains in `HELDOUT_DOMAINS` give the **schema-shift** eval set (200 pairs; `eval/schema_shift_tables.py`).

Mix 25,008 = 10,000 new + 3,000 targeted_v14 + 708 classic scalar-subquery + 5,300 other classic + 3,500 complex_v9 + 1,500 targeted_v12 + 1,000 targeted_v11; validation = 200 new + 36 classic scalar + 100 v14.

**Result (v15, 2026-10-08)**: hold-out 60/62, schema-shift 189/200 (v12-q4 155), failures with an unresolved name 42 across all sets (v14-q4 57, v12-q4 93), classic 97/97, complex 89/100; v4 hold-out 20/25. See [Evaluation](04-evaluation-and-benchmarking.md#-v15-results-2026-10-08).
