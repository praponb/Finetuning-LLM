# SQL → PySpark fine-tune

A local, Apple-Silicon pipeline that LoRA-fine-tunes **Qwen2.5-Coder-3B-Instruct (4-bit, MLX)** to translate
Spark SQL into idiomatic PySpark DataFrame code, then serves it with **Ollama** behind a small **Streamlit** UI.

> 📖 **Documentation**: [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) is the single-file guide;
> [docs/INDEX.md](docs/INDEX.md) indexes the detailed guides (architecture, data, training, evaluation, serving,
> file catalog, runbook).

**Current model:** `sql2pyspark:latest` = **v3** (since 2026-09-26): 95% correct results on 62 hand-written queries
over tables it never saw in training (v2: 90%, v1: 6%), and 95% on 300 held-out generated queries (tied with v2).
v2 and v1 are kept as `sql2pyspark:v2` / `sql2pyspark:v1`.

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
| Data | `make data` | Generate ~15k pairs + 105 hand-written pairs, execute both sides of every pair in local Spark, keep only pairs whose results match, split by feature signature. |
| Train | `make train` | `mlx_lm.lora -c sql2pyspark-mlx/lora_config.yaml` (LoRA rank 16 on the last 8 blocks, 1,200 steps × batch 4, cosine LR; ~1 h 50 min on a base M2). |
| Export | `make export TAG=v3` | Fuse adapters, dequantize, convert to Q8_0 GGUF, `ollama create sql2pyspark:v3`. |
| Evaluate | `make eval TAG=v3 PREV=v2` | Score both models on the hand-written hold-out set and the same sample of the test split. |
| Promote | `make promote TAG=v3 PREV=v2` | Point `sql2pyspark:latest` (what `app.py` uses by default) at the new model; keeps the old one as `:$(PREV)`. |
| UI | `make app` | Streamlit converter on http://localhost:8501; the sidebar field picks any Ollama tag. |

`make help` lists every target. Always pass `TAG`/`PREV` (the Makefile defaults are still `v2`/`v1`). On a 16 GB Mac,
don't run Ollama (evaluations, the app) or Docker while training: Ollama plus training ran out of GPU memory once, and
Docker Desktop's VM takes another 8 GB.

## Dataset (v3)

* **`sql2pyspark-mlx/schemas.py`**: catalog of 17 training tables and 5 *hold-out* tables (typed columns,
  foreign keys, realistic value pools) plus seeded sample data for execution checks.
* **`sql2pyspark-mlx/generate_dataset.py`** + **`sqlspec.py`**: compositional generator. Each sample picks a family
  (select, aggregation, joins, join+aggregation, windows, top-N per group, CTE + join, CTE chains, derived tables,
  IN/EXISTS/NOT EXISTS/scalar/correlated subqueries, set operations, pivot/explode/cross join, window-over-aggregate)
  and fills it with random building blocks: boolean filter trees, string/date/numeric functions, CASE, CAST,
  COALESCE, conditional aggregates, frames, HAVING, ORDER BY/LIMIT, … SQL surface style varies (case, layout,
  aliases). SQL and PySpark are rendered from the same objects, so they agree by construction.
* **`sql2pyspark-mlx/verify_pairs.py`**: runs `spark.sql(sql)` and the PySpark code on the sample tables and
  compares column names, rows and ORDER BY sequence. Only passing pairs are used (v3: all 15,105 pass).
* **`sql2pyspark-mlx/convert_sql_pyspark.py`**: chat-format JSONL. Whole feature signatures are held out for
  valid/test, so validation loss measures generalisation to unseen feature combinations.

v3 changes to the generator, aimed at the v2 hold-out failures: set operations over differently named columns
(`union()` / aliases), `GROUP BY` ordinals on `CASE` keys, and exact `NOT IN` semantics
(`isNotNull() | F.lit(excluded.isEmpty())`). The v2 dataset, adapters and logs are backed up in `sql2pyspark-mlx/runs/v2/`.

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

`eval/build_real_queries.py` → `eval/real_queries.jsonl`: real-world queries supplied by users, run against the
hand-made tables in `eval/real_tables.py` (scored with `--tables eval/real_tables.py`). It currently holds one query:
a 4-CTE marketing-attribution report (~120 lines of SQL).

* **strict**: code must run as-is and assign `result` (the convention since v2).
* **lenient**: imports added, bare `df` bound to the main table, last expression taken as the result (fair to v1).

The table below is generated by `python eval/report.py --readme README.md`. Both `test_meta` rows use the same 300-query
sample of the v3 test split; older results on the v2 split (v1 8%, v2 98%) are in `eval/results/v2-split/`.
Replies are capped at 2,048 tokens (`--num-predict`; 1,024 before 2026-09-27, which only ever cut off the real query).

<!-- RESULTS:START -->
| Model | Eval set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
|---|---|---|---|---|---|---|---|---|
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v2` | real_queries | 1 | 0% | 0% | **0%** | 0% | **0%** | 44.0 |
| `sql2pyspark:v2` | test_meta | 300 | 99% | 99% | **95%** | 99% | **95%** | 6.0 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v3` | real_queries | 1 | 0% | 0% | **0%** | 0% | **0%** | 48.8 |
| `sql2pyspark:v3` | test_meta | 300 | 100% | 98% | **95%** | 98% | **95%** | 5.3 |

Hold-out, correct results (lenient) by difficulty:

| Model | simple (n=15) | medium (n=22) | complex (n=25) |
|---|---|---|---|
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | 100% | 95% | 92% |
<!-- RESULTS:END -->

**v3 vs. v2:** v3 fixes 3 of v2's 6 hold-out failures (`GROUP BY 1` on a `CASE`, two unbalanced-parenthesis slips)
with no regressions. On the test sample the two are tied (v2 286/300, v3 284/300).

**Real-world query (2026-09-27): both v2 and v3 fail it**; neither reply even parses. The query is far outside the
training data: it is 2,371 tokens as a training sample (training samples: median 210, max 603) and uses
`GROUP BY GROUPING SETS` and `INTERVAL` arithmetic (0 training examples), `NULLS LAST` (1), `NULLIF` (5) and 4 CTEs
(training: at most 2). v3 also flattens the CTE chain into a single chain; v2 keeps named DataFrames but reuses one
window for every window function and makes up functions (`F.interval`, `F.nullIf`).

**Known gaps (v3)**, targets for a v4 dataset:

* `UNION` between columns with *different names* (`city` ∪ `hospital`) still uses `unionByName`, which fails.
* `UNION ALL` with literal columns and aliases: garbled alias syntax (`"full_name.alias("name")`).
* A misplaced parenthesis in an `EXISTS` → `left_semi` join with three conditions.
* All 9 set-operation queries in the test sample share one held-out signature (`UNION` + literal `source` column +
  `ORDER BY`): both models drop `.distinct()` after `unionByName` (v2 3/9, v3 1/9 correct).
* Long, multi-step production SQL (see the real-world query above): long CTE chains, several different windows,
  `GROUPING SETS`, `INTERVAL`, `NULLS LAST`, non-equi joins and correlated anti-joins with extra conditions.

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
sql2pyspark-mlx/   schemas.py, sqlspec.py, generate_dataset.py, handwritten_pairs.py, verify_pairs.py,
                   convert_sql_pyspark.py, lora_config.yaml, train.log (v3), raw_pairs/, data/, adapters/,
                   sql2pyspark-model/ (fused 4-bit MLX model), runs/v2/ (v2 backup)
sql2pyspark-hf/    dequantized BF16 fused model (input for GGUF conversion)
sql2pyspark.gguf   Q8_0 model served by Ollama (v3)
eval/              build_holdout.py, holdout.jsonl, build_real_queries.py, real_queries.jsonl, real_tables.py,
                   evaluate.py, report.py, results/ (+ results/v2-split/), *.log
docker/, docker-compose.yml, .dockerignore          Docker setup
llama.cpp/         upstream checkout, used only for convert_hf_to_gguf.py
archive/           previous experiments and the v1 model/data (~14 GB, safe to delete)
mlx-env/           legacy venv from the first experiment (unused)
```

`archive/` holds `my-finetuned-model/` (first experiment on non-Coder Qwen2.5-3B), `trial-3-examples/` (3-example
trial run) and `v1/` (the template-based 10k dataset, its adapters, fused model, HF export and GGUF).
`archive/`, `mlx-env/` and `sql2pyspark-hf/` (recreated by `make fuse`) can be deleted to free ~20 GB.

The code is in the private GitHub repo `praponb/Finetuning-LLM`; `.gitignore` keeps weights, generated data, venvs,
`llama.cpp/`, `archive/` and `sql2pyspark-mlx/runs/` out of git.
