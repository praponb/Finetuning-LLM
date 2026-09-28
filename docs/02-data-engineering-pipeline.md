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

```
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

### Verification Rules:
1. **Column matching**: names and order must be identical (`s_cols == p_cols`).
2. **Multiset equivalence**: rows are normalized (numbers rounded to 4 decimals, dates ISO-formatted, NaN standardized) and compared with `Counter(map(repr, rows))`, so row order doesn't matter.
3. **Ordering**: if the query has `ORDER BY`, the sequence of the ORDER BY columns must match (rows that tie may come out in either order).

### Verification Run Results (`raw_pairs/verify_report.json`):
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

### Final Split Statistics:
- **v7 (`data/split_stats.json`)**:
  - **Verified pairs**: 20,065 (minus 1 excluded matching fixed test set)
  - **Training set (`train.jsonl`)**: 16,923 pairs (9,215 signatures)
  - **Validation set (`valid.jsonl`)**: 2,095 pairs (1,198 signatures)
  - **Test set (`test.jsonl`)**: 1,047 pairs (590 signatures)
  - **Signatures shared between train and valid/test**: **0**
  - **Features missing from train**: **0**
- **v5**: verified 20,071; train 16,927 (55.1% classic, 24.5% short v4, 20.4% pipelines) / valid 2,096 / test 1,048
- **v4**: train 16,924 / valid 2,096 / test 1,048 (backup in `runs/v4/data/`)

Pipelines have nearly unique signatures, so they make up a larger share of valid/test than of train. Because the split is recomputed for every dataset version, the v2, v3 and v4 test splits contain different queries; compare two models on the same split (see [04 - Evaluation](04-evaluation-and-benchmarking.md)).

`valid_meta.jsonl` / `test_meta.jsonl` keep the SQL, reference code and ORDER BY columns for execution-based evaluation.

### Chat Template Structure:
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
