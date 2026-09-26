# 06 - File Catalog & Technical Reference

A reference for the directories, scripts, configuration files, datasets and artifacts in the repository.

---

## 🗂️ Root Files

| File | Purpose | Primary Functions / Contents | Inputs / Dependencies | Output / Artifacts |
| :--- | :--- | :--- | :--- | :--- |
| [Makefile](../Makefile) | Pipeline orchestration | Targets: `help`, `setup`, `data` (`generate`, `verify`, `split`), `train`, `fuse`, `gguf`, `ollama`, `export`, `eval`, `promote`, `app` | venv in `sql2pyspark-mlx/.venv`, Ollama | Runs the full lifecycle |
| [Modelfile](../Modelfile) | Ollama model definition | Chat template, stop token, temperature 0.2, top_p 0.9, system prompt | `sql2pyspark.gguf` | Ollama model `sql2pyspark:<tag>` |
| [README.md](../README.md) | Project summary | Pipeline, setup, conventions, results table, known gaps, layout | — | Landing page |
| [PROJECT_DOCUMENTATION.md](../PROJECT_DOCUMENTATION.md) | Single-file documentation | Condensed version of the `docs/` guides | — | — |
| [app.py](../app.py) | Streamlit web application | `extract_code(text)`, `OLLAMA_URL` | `streamlit`, `requests`, Ollama API | Web UI on `:8501` |
| [requirements.txt](../requirements.txt) | Pinned dependencies | MLX, mlx-lm, transformers, torch, gguf, PySpark, Streamlit, ... | Python 3.14 on macOS arm64 (tested) | Contents of the project venv |
| [pyrefly.toml](../pyrefly.toml) | Editor type-checker config | Points Pyrefly at `sql2pyspark-mlx/.venv/bin/python`; adds `sql2pyspark-mlx/` to the search path; excludes vendored/generated folders | Pyrefly extension | No red "Cannot find module" errors |
| `sql2pyspark.gguf` | Quantized model (v3) | 3.29 GB, Q8_0 | Output of `make gguf` | Loaded by Ollama |
| [.gitignore](../.gitignore) | Git exclusions | venvs, caches, weights (`*.gguf`, `*.safetensors`, adapters, fused models, `sql2pyspark-hf/`), `archive/`, generated data (`raw_pairs/`, `data/`), `llama.cpp/`, `sql2pyspark-mlx/runs/`, `.agents/`, `.claude/` | Git; private GitHub repo `praponb/Finetuning-LLM` | Keeps the repo to source, configs, docs, eval results and logs |
| [docker-compose.yml](../docker-compose.yml) | Docker services | `app` (Streamlit), `tools` profile (Spark data/eval), `ollama` profile | Docker Desktop | See the [runbook](07-operations-runbook.md#-docker) |
| [.dockerignore](../.dockerignore) | Docker build-context allowlist | Only `app.py` and `docker/` are sent to the builder | Docker | Keeps ~28 GB of models/data out of builds |
| `docker/` | Docker files (both images on `python:3.12-slim-bookworm`) | `app.Dockerfile`, `tools.Dockerfile`, `requirements-app.txt`, `requirements-tools.txt`, `ollama-entrypoint.sh` | Docker | Container images |

---

## 🔬 Data Synthesis & Training Module (`sql2pyspark-mlx/`)

| File / Folder | Purpose | Key Symbols / Details | Data / Outputs |
| :--- | :--- | :--- | :--- |
| [schemas.py](../sql2pyspark-mlx/schemas.py) | Table catalog & sample data | `Column`, `Table`, `TABLES`, `make_rows()`, `make_dataframes()`, `train_tables()`, `holdout_tables()` | 17 training tables + 5 hold-out tables; seeded 40-row DataFrames |
| [sqlspec.py](../sql2pyspark-mlx/sqlspec.py) | Dual-rendering query objects | Expressions `E`, `Col`, `Lit`, `Raw`, `BinOp`, `Fn`, `Case`, `Cast`, `Agg`, `WinFn`; predicates `P`, `Cmp`, `InList`, `Between`, `Like`, `IsNull`, `And`, `Or`, `Not`; query objects `WinSpec`, `Rel`, `Join`, `Semi`, `Sel`; `Style`, `Ctx`, `render_code()` | Matching `.sql(style)` and `.py(ctx)` text from one object tree |
| [generate_dataset.py](../sql2pyspark-mlx/generate_dataset.py) | Compositional query generator | Families `fam_select`, `fam_agg`, `fam_join`, `fam_join_agg`, `fam_window`, `fam_topn`, `fam_cte_agg_join`, `fam_cte_chain`, `fam_derived`, `fam_subquery`, `fam_setop`, `fam_special`; helpers `atom()`, `where()`, `derive()`, `make_agg()`, `make_key()`, `add_order()` | `raw_pairs/genNNNNN.sql` + `.py`, `meta_generated.jsonl` (deterministic for a given `--seed`) |
| [handwritten_pairs.py](../sql2pyspark-mlx/handwritten_pairs.py) | Curated pairs | Helper `P(sql, body, order)`, list `PAIRS` (105 pairs) | `raw_pairs/hwNNN.sql` + `.py`, `meta_handwritten.jsonl` |
| [verify_pairs.py](../sql2pyspark-mlx/verify_pairs.py) | Spark dual-execution check | `Checker`, `Result`, `compare()`, `norm()`, `rows_of()`, multiprocess CLI | `verified.jsonl`, `verify_report.json`, `verify_failures.jsonl` |
| [convert_sql_pyspark.py](../sql2pyspark-mlx/convert_sql_pyspark.py) | Signature-based splitter | `signature(rec)`, `split()`, `format_sample()`, `write_jsonl()` | `data/train.jsonl`, `valid.jsonl`, `test.jsonl`, `*_meta.jsonl`, `split_stats.json` |
| [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) | MLX fine-tuning parameters | Rank 16, 8 layers, 1,200 iters, batch 4, AdamW, cosine schedule | Read by `mlx_lm.lora` |
| `train.log` | Training log (v3 run) | Loss, learning rate, speed, tokens, peak memory per report | Training history |
| `data_v3_docker.log` | v3 dataset build log | `make data` run inside the `tools` container | Record of the v3 data build |
| `data/` | Split dataset (v3) | `train.jsonl` (12,743), `valid.jsonl` (1,575), `test.jsonl` (787), `valid_meta.jsonl`, `test_meta.jsonl`, `split_stats.json` | MLX training data + evaluation metadata |
| `raw_pairs/` | Generated pairs (v3) | 15,105 `.sql`/`.py` pairs, `meta_*.jsonl`, `verified.jsonl`, `verify_report.json`, `verify_failures.jsonl`, `verify.log` | Input to `verify_pairs.py` and `convert_sql_pyspark.py` |
| `adapters/` | LoRA weights (v3) | `adapters.safetensors`, checkpoints `0000300_` … `0001200_adapters.safetensors`, `adapter_config.json` | Produced by `mlx_lm.lora`; used by `mlx_lm.fuse` |
| `sql2pyspark-model/` | Fused 4-bit MLX model (v3) | `model.safetensors` (1.74 GB), `config.json`, tokenizer files, model card `README.md` | Native MLX model |
| `runs/v2/` | v2 backup (git-ignored, ~270 MB) | `adapters/`, `data/`, `raw_pairs/`, `lora_config.yaml`, `train.log`, `post_training.log` | Rollback / comparison with v2 |
| `.venv/` | Project Python environment | Python 3.14 + `requirements.txt` | Used by the Makefile and Pyrefly |

---

## 📦 Dequantized Export (`sql2pyspark-hf/`, v3)

| File | Purpose | Size / Format | Role in Pipeline |
| :--- | :--- | :--- | :--- |
| `config.json` | Model architecture | JSON (712 B) | Qwen2 causal LM parameters (`torch_dtype: bfloat16`) |
| `chat_template.jinja` | Chat template | Jinja (2.5 KB) | ChatML formatting with `<\|im_start\|>` |
| `tokenizer.json` | BPE tokenizer | JSON (11.4 MB) | Qwen2.5 tokenizer |
| `tokenizer_config.json` | Tokenizer settings | JSON (729 B) | Special tokens |
| `model-00001-of-00002.safetensors` | BF16 weights, shard 1 | 5.36 GB | Input for GGUF conversion |
| `model-00002-of-00002.safetensors` | BF16 weights, shard 2 | 816 MB | Input for GGUF conversion |
| `model.safetensors.index.json` | Shard index | JSON (37 KB) | Maps tensors to shards |
| `README.md` | Model card | Markdown | Metadata, licence and short description |

---

## 🧪 Evaluation Module (`eval/`)

| File | Purpose | Key Symbols / Details | Role in Pipeline |
| :--- | :--- | :--- | :--- |
| [build_holdout.py](../eval/build_holdout.py) | Builds the hold-out benchmark | Helper `H(level, sql, body, order)`, list `CASES` (62 cases) | Writes `eval/holdout.jsonl` |
| `holdout.jsonl` | Hold-out benchmark | 62 entries (15 simple, 22 medium, 25 complex) on unseen tables | Read by `evaluate.py` |
| [evaluate.py](../eval/evaluate.py) | Execution-based evaluator | `ask()`, `extract_code()`, `lenient()`, `score()`, `main_table()`; `--sample`, `--reuse-replies`, `OLLAMA_URL` | Queries Ollama, runs code in Spark, scores results |
| [report.py](../eval/report.py) | Results-table generator | `load()`, `table()`; `--readme` replaces the block between `RESULTS` markers | Markdown tables from `eval/results/*.json` |
| `results/` | Evaluation results | `v1__holdout.json`, `v2__holdout.json`, `v3__holdout.json`, `v2__test.json`, `v3__test.json` (every reply + score; both test files use the same 300-query sample of the v3 split) | Source for `report.py` |
| `results/v2-split/` | Earlier test results | `v1__test.json`, `v2__test.json` on the v2 split | Kept for reference; not read by `report.py` |
| `results_v1_holdout.log`, `results_v1_test.log` | v1 evaluation console logs | Progress and summary lines | Reference |
| `post_training.log` | Post-training run log (v2) | Output of `make fuse`, `make gguf`, `make ollama`, and the v1/v2 evaluations | Record of the v2 export |
| `post_training_v3.log` | Post-training run log (v3) | v3 export, then the v3 hold-out/test and v2 test evaluations | Record of the v3 export |

---

## 🏛️ Historical Archives (`archive/`, ~14 GB)

| Directory | Contents | Status |
| :--- | :--- | :--- |
| `archive/v1/` | Template-based v1: scripts (`generate_10k_dataset.py`, `generate_100_pairs.py`, `prepare_dataset.py`), 10k-pair dataset, adapters, fused model, HF export, GGUF | v1 baseline; `sql2pyspark:v1` in Ollama keeps its own copy, so this folder is safe to delete |
| `archive/my-finetuned-model/` | First fine-tune, on the non-Coder `Qwen2.5-3B-Instruct` | Historical, never evaluated |
| `archive/trial-3-examples/` | Trial run with 3 training examples (adapters, data, fused model) | Historical |

Other folders: `llama.cpp/` (upstream checkout, used only for `convert_hf_to_gguf.py`) and `mlx-env/` (legacy venv from the first experiment, unused).

`archive/` (~14 GB) and `mlx-env/` (~0.4 GB) are safe to delete. `sql2pyspark-hf/` (~5.8 GB) is only needed as input for `make gguf` and is recreated by `make fuse`.
