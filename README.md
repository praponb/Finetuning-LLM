# SQL → PySpark fine-tune

A local, Apple-Silicon pipeline that LoRA-fine-tunes **Qwen2.5-Coder-3B-Instruct (4-bit, MLX)** to translate
Spark SQL into idiomatic PySpark DataFrame code, then serves it with **Ollama** behind a small **Streamlit** UI.

> 📖 **Documentation**: [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) is the single-file guide;
> [docs/INDEX.md](docs/INDEX.md) indexes the detailed guides (architecture, data, training, evaluation, serving,
> file catalog, runbook).

**Current model:** `sql2pyspark:latest` = **v5** (since 2026-09-28). v5 continued from v4 on a replay-heavy dataset with
generator fixes and is the strongest model overall: 128/150 on a fixed test set (v3 105, v4 110) with v3's accuracy on
classic queries (95/97 vs. 96/97), and 14/25 on the v4-feature hold-out set (v4 9, v3 3). It scores 56/62 on the
original hold-out (v3 59) and 0/4 on the real-world queries (like every model), so it misses two conditions of the
promotion rule; it was promoted anyway by the owner's decision. See [v5 results](#v5-results-2026-09-28).
**v3** (the previous `latest`, 95% on the hold-out) is kept as `sql2pyspark:v3`: roll back with
`ollama cp sql2pyspark:v3 sql2pyspark:latest`. v4, v2 and v1 are kept as `sql2pyspark:v4` / `:v2` / `:v1`.

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
| Train | `make train` | `train_resumable.py` runs `mlx_lm.lora -c sql2pyspark-mlx/lora_config.yaml` and resumes from the last checkpoint if Metal aborts a step (LoRA rank 16 on the last 8 blocks, 1,200 updates × 4 samples as batch 2 × 2 accumulation, gradient checkpointing, max 3,200 tokens; v5 continued from v4's adapters with peak LR 5e-5; ~5 h 40 min on a base M2). |
| Export | `make export TAG=v4` | Fuse adapters, dequantize, convert to Q8_0 GGUF, `ollama create sql2pyspark:v4`. |
| Evaluate | `make eval TAG=v4 PREV=v3` | Score both models on the hold-out set, the v4 hold-out set, the real-world queries and the same sample of the test split. |
| Promote | `make promote TAG=v4 PREV=v3` | Point `sql2pyspark:latest` (what `app.py` uses by default) at the new model; keeps the old one as `:$(PREV)`. |
| UI | `make app` | Streamlit converter on http://localhost:8501; the sidebar field picks any Ollama tag. |

`make help` lists every target. The Makefile defaults are `TAG=v4` and `PREV=v3`. On a 16 GB Mac,
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
  `fam_pipeline` chains 4-8 stages into production-style queries of up to ~2.7k tokens.: runs `spark.sql(sql)` and the PySpark code on the sample tables and
  compares column names, rows and ORDER BY sequence. Only passing pairs are used (v4: 20,068 of 20,105; v3: all 15,105).
* **`sql2pyspark-mlx/convert_sql_pyspark.py`**: chat-format JSONL. Whole feature signatures are held out for
  valid/test, so validation loss measures generalisation to unseen feature combinations.

v4 dataset: 20,068 verified pairs (train 16,924 / valid 2,096 / test 1,048); 42% come from the v4 families and 16%
are long pipelines (median ~1,100 tokens, max 3,119; the v3 data maxed at 603). `UNION` (with `.distinct()`) is now
drawn more often than `UNION ALL`.

v3 changes to the generator, aimed at the v2 hold-out failures: set operations over differently named columns
(`union()` / aliases), `GROUP BY` ordinals on `CASE` keys, and exact `NOT IN` semantics
(`isNotNull() | F.lit(excluded.isEmpty())`). The v2 and v3 datasets, adapters and logs are backed up in
`sql2pyspark-mlx/runs/v2/` and `runs/v3/`.

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
<!-- RESULTS:END -->

**v3 vs. v2:** v3 fixes 3 of v2's 6 hold-out failures (`GROUP BY 1` on a `CASE`, two unbalanced-parenthesis slips)
with no regressions. On the test sample the two are tied (v2 286/300, v3 284/300).

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
| Test sample (300, v4 split) | 105/150 (70.0%) | 110/150 (73.3%) | On the 150-query test sample v4 is 5 queries ahead overall, but only because it gains 17 on the v4 families (26/53 vs. 9/53; long pipelines 5/22 vs. 0/22) while losing 12 on the classic v1–v3 families (84/97 vs. 96/97: 5 syntax errors, 3 runtime errors, 2 wrong column lists, 2 wrong row sets), so v4 is not a safe drop-in replacement either. |

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

**Agent repair with Kimi K3 (2026-09-28)**: `eval/agent_repair.py` gives Kimi K3 (Moonshot AI) each real query, v5's
answer and one tool that runs PySpark on the sample tables (no expected output, no `.sql(...)`, at most 5 runs). It fixes
3 of the 4 real queries (0/4 before; all 4 run) for $0.90 in total, about 5 minutes per query. `real003` still keeps
NULL rows that Spark SQL's `UNPIVOT` drops by default. Needs `KIMI_API_KEY` in `.env` (template `.env.example`; details in
docs/04).

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
docker compose --profile tools run --rm tools make eval PY=python TAG=v4 PREV=v3   # evaluation against the host's Ollama
OLLAMA_URL=http://ollama:11434 docker compose --profile ollama up -d --build      # Ollama in a container (Linux servers)
```

Verified: the app container, the v3 dataset build in `tools` (15,105/15,105 pairs), and the v3 hold-out evaluation in
`tools` (59/62, identical replies and scores to the native run). Training cannot run in Docker on a Mac (MLX needs
Metal, which containers can't use), and Ollama in a container is CPU-only there, so native Ollama stays the recommended
way to serve the model on macOS. Stop Docker before `make train`.

## Layout

```
app.py, Modelfile, Makefile, requirements.txt, pyrefly.toml, .gitignore
README.md, PROJECT_DOCUMENTATION.md, docs/          documentation
sql2pyspark-mlx/   schemas.py, sqlspec.py, generate_dataset.py, advanced.py (v4 families), handwritten_pairs.py,
                   verify_pairs.py, convert_sql_pyspark.py, lora_config.yaml, train_resumable.py, train.log (v5),
                   data_v5.log, raw_pairs/, data/, adapters/, sql2pyspark-model/ (fused 4-bit MLX model),
                   runs/v2/, runs/v3/, runs/v4/ (backups)
sql2pyspark-hf/    dequantized BF16 fused model (input for GGUF conversion; now v5)
sql2pyspark.gguf   Q8_0 GGUF of the last export (v5); Ollama keeps its own copy of every tag
eval/              build_holdout.py, holdout.jsonl, build_holdout_v4.py, holdout_v4.jsonl, build_real_queries.py,
                   real_queries.jsonl, real_tables.py, test_fixed.jsonl, evaluate.py, report.py,
                   post_training_v4.sh, post_training_v5.sh, results/ (+ v2-split/, v3-split/, v4-split/), *.log
docker/, docker-compose.yml, .dockerignore          Docker setup
llama.cpp/         upstream checkout, used only for convert_hf_to_gguf.py
```

The Docker `ollama` profile registers the model from the mounted `sql2pyspark.gguf`, i.e. the last export (v5).

`archive/` (earlier experiments and the v1 model/data) and the legacy `mlx-env/` venv were deleted on 2026-09-27;
`sql2pyspark-hf/` can be deleted too (recreated by `make fuse`).

The code is in the private GitHub repo `praponb/Finetuning-LLM`; `.gitignore` keeps weights, generated data, venvs,
`llama.cpp/`, `archive/` and `sql2pyspark-mlx/runs/` out of git.
