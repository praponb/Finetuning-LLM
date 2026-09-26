# SQL → PySpark fine-tune

A local, Apple-Silicon pipeline that LoRA-fine-tunes **Qwen2.5-Coder-3B-Instruct (4-bit, MLX)** to translate
Spark SQL into idiomatic PySpark DataFrame code, then serves it with **Ollama** behind a small **Streamlit** UI.

> 📖 **Documentation**: [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) is the single-file guide;
> [docs/INDEX.md](docs/INDEX.md) indexes the detailed guides (architecture, data, training, evaluation, serving,
> file catalog, runbook).

**Current model:** `sql2pyspark:latest` = **v2** — 90% correct results on 62 hand-written queries over tables it
never saw in training (v1: 6%), 98% on 300 held-out generated queries (v1: 8%). v1 is kept as `sql2pyspark:v1`.

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
| Export | `make export TAG=v2` | Fuse adapters, dequantize, convert to Q8_0 GGUF, `ollama create sql2pyspark:v2`. |
| Evaluate | `make eval TAG=v2 PREV=v1` | Score both models on the hand-written hold-out set and a sample of the test split. |
| Promote | `make promote TAG=v2` | Point `sql2pyspark:latest` (what `app.py` uses by default) at the new model; keeps the old one as `:v1`. |
| UI | `make app` | Streamlit converter on http://localhost:8501; the sidebar field picks any Ollama tag. |

`make help` lists every target. On a 16 GB Mac, don't run Ollama (evaluations, the app) while training — the two
together ran out of GPU memory once.

## Dataset (v2)

* **`sql2pyspark-mlx/schemas.py`**: catalog of 17 training tables and 5 *hold-out* tables (typed columns,
  foreign keys, realistic value pools) plus seeded sample data for execution checks.
* **`sql2pyspark-mlx/generate_dataset.py`** + **`sqlspec.py`**: compositional generator. Each sample picks a family
  (select, aggregation, joins, join+aggregation, windows, top-N per group, CTE + join, CTE chains, derived tables,
  IN/EXISTS/NOT EXISTS/scalar/correlated subqueries, set operations, pivot/explode/cross join, window-over-aggregate)
  and fills it with random building blocks: boolean filter trees, string/date/numeric functions, CASE, CAST,
  COALESCE, conditional aggregates, frames, HAVING, ORDER BY/LIMIT, … SQL surface style varies (case, layout,
  aliases). SQL and PySpark are rendered from the same objects, so they agree by construction.
* **`sql2pyspark-mlx/verify_pairs.py`**: runs `spark.sql(sql)` and the PySpark code on the sample tables and
  compares column names, rows and ORDER BY sequence. Only passing pairs are used (15,104 of 15,105).
* **`sql2pyspark-mlx/convert_sql_pyspark.py`**: chat-format JSONL. Whole feature signatures are held out for
  valid/test, so validation loss measures generalisation to unseen feature combinations.

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
* `UNION ALL` → `unionByName`, `UNION` → `unionByName(...).distinct()`, `EXCEPT` → `subtract`;
* CTEs become named intermediate DataFrames; scalar subqueries become a value computed up front.

## Evaluation

`eval/build_holdout.py` → `eval/holdout.jsonl`: 62 hand-written queries (15 simple, 22 medium, 25 complex) over the
hold-out tables, each with a verified reference answer. `eval/evaluate.py` sends each query to an Ollama model with
the same prompt as `app.py`, executes the returned code and compares it with the SQL result.

* **strict**: code must run as-is and assign `result` (the v2 convention).
* **lenient**: imports added, bare `df` bound to the main table, last expression taken as the result (fair to v1).

The table below is generated by `python eval/report.py --readme README.md`.

<!-- RESULTS:START -->
| Model | Eval set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
|---|---|---|---|---|---|---|---|---|
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v2` | test_meta | 300 | 99% | 99% | **95%** | 99% | **95%** | 6.0 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v3` | test_meta | 300 | 100% | 98% | **95%** | 98% | **95%** | 5.3 |

Hold-out, correct results (lenient) by difficulty:

| Model | simple (n=15) | medium (n=22) | complex (n=25) |
|---|---|---|---|
| `sql2pyspark:v1` | 7% | 14% | 0% |
| `sql2pyspark:v2` | 93% | 95% | 84% |
| `sql2pyspark:v3` | 100% | 95% | 92% |
<!-- RESULTS:END -->

**Known gaps (v2 hold-out failures)**, useful targets for the next dataset iteration:

* `UNION` between columns with *different names* (`city` ∪ `hospital`): the model uses `unionByName`, which fails,
  because the generator only produces set operations over identically named columns. Fix: add pairs that alias or use `union()`.
* `GROUP BY 1` where column 1 is a `CASE` expression: the CASE was dropped (grouped by `YEAR(...)` instead).
* Occasional unbalanced parentheses in long single-line expressions (`SUM(SUM(x)) OVER ()`, `/ 365` after a function).
* One hallucinated join type (`"left_seats"`) on an `EXISTS` with three conditions.

## Docker (work in progress)

Docker files exist but the setup is **not finished or tested yet**:

* `docker-compose.yml` defines an `app` service (Streamlit, talks to the host's Ollama via `OLLAMA_URL`), a `tools`
  profile (Java + PySpark for `make data` / `make eval`), and an optional `ollama` profile (Ollama in a container).
* The `tools` image build currently fails: `python:3.12-slim` is now Debian 13, which has no `openjdk-17` package.
  Pending fix: base both images on `python:3.12-slim-bookworm`.
* Training cannot run in Docker on a Mac (MLX needs Metal, which containers can't use), and Ollama in a container is
  CPU-only there, so native Ollama stays the recommended way to serve the model on macOS.

## Layout

```
app.py, Modelfile, Makefile, requirements.txt, pyrefly.toml
README.md, PROJECT_DOCUMENTATION.md, docs/          documentation
sql2pyspark-mlx/   schemas.py, sqlspec.py, generate_dataset.py, handwritten_pairs.py, verify_pairs.py,
                   convert_sql_pyspark.py, lora_config.yaml, train.log, raw_pairs/, data/, adapters/,
                   sql2pyspark-model/ (fused 4-bit MLX model)
sql2pyspark-hf/    dequantized BF16 fused model (input for GGUF conversion)
sql2pyspark.gguf   Q8_0 model served by Ollama
eval/              build_holdout.py, holdout.jsonl, evaluate.py, report.py, results/, *.log
docker/, docker-compose.yml, .dockerignore          Docker setup (work in progress)
llama.cpp/         upstream checkout, used only for convert_hf_to_gguf.py
archive/           previous experiments and the v1 model/data (~14 GB, safe to delete)
mlx-env/           legacy venv from the first experiment (unused)
```

`archive/` holds `my-finetuned-model/` (first experiment on non-Coder Qwen2.5-3B), `trial-3-examples/` (3-example
trial run) and `v1/` (the template-based 10k dataset, its adapters, fused model, HF export and GGUF).
