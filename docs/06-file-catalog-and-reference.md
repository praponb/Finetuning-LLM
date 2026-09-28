# 06 - File Catalog & Technical Reference

A reference for the directories, scripts, configuration files, datasets and artifacts in the repository.

---

## 🗂️ Root Files

| File | Purpose | Primary Functions / Contents | Inputs / Dependencies | Output / Artifacts |
| :--- | :--- | :--- | :--- | :--- |
| [Makefile](../Makefile) | Pipeline orchestration | Targets: `help`, `setup`, `data` (`generate`, `verify`, `split`), `train` (via `train_resumable.py`), `fuse`, `gguf`, `ollama`, `export`, `eval`, `promote`, `app` (`TAG ?= v6`, `PREV ?= v5`, `BASE ?= 7B`) | venv in `sql2pyspark-mlx/.venv`, Ollama | Runs the full lifecycle |
| [Modelfile](../Modelfile) | Ollama model definition | Chat template, stop token, temperature 0.2, top_p 0.9, system prompt | `sql2pyspark.gguf` | Ollama model `sql2pyspark:<tag>` |
| [USER_MANUAL.md](../USER_MANUAL.md) | End-user manual & guide | Streamlit UI walkthrough, CLI usage, REST API (`curl`/Python), SQL feature matrix, AST validation, Kimi K3 repair, FAQ | — | End-user manual |
| [README.md](../README.md) | Project summary | Pipeline, setup, conventions, results table, known gaps, layout | — | Landing page |
| [PROJECT_DOCUMENTATION.md](../PROJECT_DOCUMENTATION.md) | Single-file documentation | Condensed version of the `docs/` guides | — | — |
| [app.py](../app.py) | Streamlit web application | `extract_code(text)`, `generation_options(sql)` (long-SQL repetition window), `OLLAMA_URL`, Kimi K3 empty-table check & repair | `streamlit`, `requests`, `pyspark`, Ollama API | Web UI on `:8501` |
| [kimi_repair.py](../kimi_repair.py) | Kimi K3 repair loop (shared) | `static_check()`, `repair()`, `EmptyTableRunner` (empty tables with Kimi-declared schemas), `load_env()`, `make_client()`, token pricing | Used by `app.py` and `eval/agent_repair.py` | AST checks, Spark empty table runner, Moonshot API client |
| [requirements.txt](../requirements.txt) | Pinned dependencies | MLX, mlx-lm, transformers, torch, gguf, PySpark, Streamlit, openai, ... | Python 3.14 on macOS arm64 (tested) | Contents of the project venv |
| [pyrefly.toml](../pyrefly.toml) | Editor type-checker config | Points Pyrefly at `sql2pyspark-mlx/.venv/bin/python`; adds `sql2pyspark-mlx/` to the search path; excludes vendored/generated folders | Pyrefly extension | No red "Cannot find module" errors |
| `sql2pyspark.gguf` | Quantized model of the last completed export (v6, 7B) | 8.10 GB, Q8_0 (3B: 3.29 GB) | Output of `make gguf` | Registered in Ollama by `make ollama`; Ollama keeps its own copy of every tag (`latest` = v5) |
| `.env.example` | API-key template | `KIMI_API_KEY`, `KIMI_BASE_URL`, `KIMI_MODEL`; copy to `.env` (gitignored) | Read by `app.py` and `eval/agent_repair.py` | API credentials for Kimi K3 repair |
| [.gitignore](../.gitignore) | Git exclusions | `.env`, venvs, caches, weights (`*.gguf`, `*.safetensors`, adapters, fused models, `sql2pyspark-hf/`), `archive/`, generated data (`raw_pairs/`, `data/`), `llama.cpp/`, `sql2pyspark-mlx/runs/`, `.agents/`, `.claude/` | Git; private GitHub repo `praponb/Finetuning-LLM` | Keeps the repo to source, configs, docs, eval results and logs |
| [docker-compose.yml](../docker-compose.yml) | Docker services | `app` (Streamlit + Java 17 + PySpark), `tools` profile (Spark data/eval), `ollama` profile | Docker Desktop | See the [runbook](07-operations-runbook.md#-docker) |
| [.dockerignore](../.dockerignore) | Docker build-context allowlist | Only `app.py` and `docker/` are sent to the builder | Docker | Keeps ~28 GB of models/data out of builds |
| `docker/` | Docker files (both images on `python:3.12-slim-bookworm`) | `app.Dockerfile`, `tools.Dockerfile`, `requirements-app.txt`, `requirements-tools.txt`, `ollama-entrypoint.sh` | Docker | Container images |

---

## 🔬 Data Synthesis & Training Module (`sql2pyspark-mlx/`)

| File / Folder | Purpose | Key Symbols / Details | Data / Outputs |
| :--- | :--- | :--- | :--- |
| [schemas.py](../sql2pyspark-mlx/schemas.py) | Table catalog & sample data | `Column`, `Table`, `TABLES`, `EXTRA_TABLES`, `make_rows()`, `make_dataframes()`, `train_tables()`, `holdout_tables()`, `extra_train_tables()` | 17 base + 5 extra train + 5 base + 1 extra hold-out tables; seeded DataFrames |
| [sqlspec.py](../sql2pyspark-mlx/sqlspec.py) | Dual-rendering query objects | Expressions `E`, `Col`, `Lit`, `Raw`, `BinOp`, `Fn`, `Case`, `Cast`, `Agg`, `WinFn`; predicates `P`, `Cmp`, `InList`, `Between`, `Like`, `IsNull`, `And`, `Or`, `Not`; query objects `WinSpec`, `Rel`, `Join`, `Semi`, `Sel`; `Style`, `Ctx`, `render_code()` | Matching `.sql(style)` and `.py(ctx)` text from one object tree |
| [generate_dataset.py](../sql2pyspark-mlx/generate_dataset.py) | Compositional query generator | Families `fam_select`, `fam_agg`, `fam_join`, `fam_join_agg`, `fam_window`, `fam_topn`, `fam_cte_agg_join`, `fam_cte_chain`, `fam_derived`, `fam_subquery`, `fam_setop`, `fam_special`; helpers `atom()`, `where()`, `derive()`, `make_agg()`, `make_key()`, `add_order()` | `raw_pairs/genNNNNN.sql` + `.py`, `meta_generated.jsonl` (deterministic for a given `--seed`) |
| [advanced.py](../sql2pyspark-mlx/advanced.py) | v4/v5 stage-based families | Stages `stg_clean`, `stg_dedup`, `stg_windows`, `stg_session`, `stg_join`, `stg_lateral`, `stg_anti`, `stg_setop`, `stg_agg`, `stg_join_agg` (v5), `stg_pivot`, `stg_unpivot`, `stg_final`, `stg_explode`, `stg_nested_select`; families `fam_adv_*` and `fam_pipeline`; `run_pipeline()`, `try_stages()`, `bind()` | Appended to `generate_dataset.FAMILIES` (same `raw_pairs/` output) |
| [handwritten_pairs.py](../sql2pyspark-mlx/handwritten_pairs.py) | Curated pairs | Helper `P(sql, body, order)`, list `PAIRS` (105 pairs) | `raw_pairs/hwNNN.sql` + `.py`, `meta_handwritten.jsonl` |
| [verify_pairs.py](../sql2pyspark-mlx/verify_pairs.py) | Spark dual-execution check | `Checker`, `Result`, `compare()`, `norm()`, `rows_of()`, multiprocess CLI | `verified.jsonl`, `verify_report.json`, `verify_failures.jsonl` |
| [convert_sql_pyspark.py](../sql2pyspark-mlx/convert_sql_pyspark.py) | Signature-based splitter | `signature(rec)`, `split()`, `format_sample()`, `write_jsonl()` | `data/train.jsonl`, `valid.jsonl`, `test.jsonl`, `*_meta.jsonl`, `split_stats.json` |
| [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) | MLX fine-tuning parameters (v6) | 7B model, fresh adapters, 4,800 micro-batches (batch 1 × 4 accumulation), max 3,200 tokens, grad checkpointing, 8 of 28 layers, AdamW cosine schedule | Read by `mlx_lm.lora` |
| [train_resumable.py](../sql2pyspark-mlx/train_resumable.py) | Crash-proof training wrapper (`make train`) | Smaller Metal command buffers (`MLX_MAX_OPS_PER_BUFFER`, `MLX_MAX_MB_PER_BUFFER`); resumes from newest checkpoint with cosine schedule continued; `lr_at()` | Adapters in `adapters/` |
| `train.log` | Training log (v6, 07:09 → 15:28, validation loss 0.010) | Loss, learning rate, speed, tokens, peak memory per report | Active training progress |
| `data_v3_docker.log` | v3 dataset build log | `make data` run inside the `tools` container | Record of the v3 data build |
| `data_v5.log` | v5 dataset build log | Native `make data N=20000 SEED=5 WORKERS=4` (generate, verify, split with `--exclude`) | Record of the v5 data build |
| `data_v4.log` | v4 dataset build log | Native `make data N=20000 WORKERS=4` (generate, verify, split) | Record of the v4 data build |
| `data/` | Split dataset (v5) | `train.jsonl` (16,927), `valid.jsonl` (2,096), `test.jsonl` (1,048), `valid_meta.jsonl`, `test_meta.jsonl`, `split_stats.json` | MLX training data + evaluation metadata |
| `raw_pairs/` | Generated pairs (v5) | 20,105 `.sql`/`.py` pairs, `meta_*.jsonl`, `verified.jsonl` (20,073), `verify_report.json`, `verify_failures.jsonl`, `verify.log` | Input to `verify_pairs.py` and `convert_sql_pyspark.py` |
| `adapters/` | LoRA weights (v6) | `adapters.safetensors`, checkpoints `0000200_` … `adapters.safetensors`, `adapter_config.json` | Produced by `mlx_lm.lora`; used by `mlx_lm.fuse` |
| `sql2pyspark-model/` | Fused 4-bit MLX model (v6) | `model.safetensors` (4.28 GB; 3B: 1.74 GB), `config.json`, tokenizer files, model card `README.md` | Native MLX model |
| `runs/v2/` … `runs/v5/` | Version backups (git-ignored) | `adapters/`, `data/`, `raw_pairs/`, `lora_config.yaml`, `train.log`, post-training logs | Rollback / comparison across versions |
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
| [build_holdout_v4.py](../eval/build_holdout_v4.py) | Builds the v4 hold-out benchmark | Helper `H(level, sql, body, order)`, list `CASES` (25 cases, 7 feature groups); checks each reference in Spark | Writes `eval/holdout_v4.jsonl` |
| [build_real_queries.py](../eval/build_real_queries.py) | Builds the real-world query set | Helper `R(name, sql, body, order)`, list `CASES` (4 queries); checks each reference against its SQL in Spark | Writes `eval/real_queries.jsonl` |
| `real_queries.jsonl` | Real-world query set | 4 user-supplied queries (`real001`–`real004`) | Read by `evaluate.py` with `--tables eval/real_tables.py` |
| [real_tables.py](../eval/real_tables.py) | Sample tables for the real-world queries | `raw_events`, `marketing_campaigns`, `fraud_blacklist`, `orders`, `warehouse_inventory`, `blacklisted_fulfillments`, `warehouse_sla`, `sensor_telemetry`, `calibration_registry`, `operational_incidents`, `device_metadata`, `trade_stream`, `cleared_settlement_batches`; `make_dataframes(spark)` | Loaded by `evaluate.py --tables` and `build_real_queries.py` |
| `holdout.jsonl` | Hold-out benchmark | 62 entries (15 simple, 22 medium, 25 complex) on unseen tables | Read by `evaluate.py` |
| `test_fixed.jsonl` | Fixed test set (since v5) | The 150-query sample of the v4 test split (id, family, SQL, reference, ORDER BY columns); excluded from later training data | Read by `evaluate.py`; `convert_sql_pyspark.py --exclude` |
| `holdout_v4.jsonl` | v4 hold-out benchmark | 25 entries on unseen tables incl. the nested `vehicle_trips`; `level` = feature group | Read by `evaluate.py` |
| [evaluate.py](../eval/evaluate.py) | Execution-based evaluator | `ask()`, `extract_code()`, `lenient()`, `score()`, `main_table()`, `load_tables()`; `--sample`, `--reuse-replies`, `--tables`, `--num-predict`, `--option KEY=VALUE`, `OLLAMA_URL` | Queries Ollama, runs code in Spark, scores results |
| [report.py](../eval/report.py) | Results-table generator | `load()`, `table()`; `--readme` replaces the block between `RESULTS` markers | Markdown tables from `eval/results/*.json` |
| `results/` | Evaluation results | `<tag>__<set>.json` for `holdout`, `holdout_v4`, `real_queries` and `test` (every reply + score); `v3__test.json` and `v4__test.json` use the same 300-query sample of the v4 split | Source for `report.py` |
| `results/v2-split/` | Earlier test results | `v1__test.json`, `v2__test.json` on the v2 split | Kept for reference; not read by `report.py` |
| [agent_repair.py](../eval/agent_repair.py) | Agent repair evaluation (Kimi K3) | `preview()` (sample-table tool), `table_schemas()`; `--mode sample\|empty`, `--results`, `--max-runs`, `--effort`, `--only`, `--dry-run` | Reads a results JSON, runs `kimi_repair.repair()`, scores the repaired code |
| `results/v5-repair/` | Kimi K3 repair of v5's real-query answers | `real_queries.json` (sample tables), `real_queries_empty.json` (empty tables, like the app); per query: before/after score, tool runs, tokens, cost | See docs/04; logs `eval/agent_repair_v5*.log` |
| `results/v6-repeat-penalty/` | v6 with the app's long-SQL setting | `A2_rp1.1_2048__real_queries.json` (`repeat_last_n=2048`) | Shows the setting doesn't stop v6's `real001` loop |
| `results/v5-repeat-penalty/` | v5 with stronger repetition settings | `A2_rp1.1_2048__*.json`, `B_rp1.2_256__*.json`, `C_rp1.3_256__*.json` (real queries + hold-out) | Evidence for the long-SQL setting in `app.py` |
| `results/v4-split/` | v3/v4 results on the v4-split sample (identical to their `__test_fixed.json`) | `v3__test.json`, `v4__test.json` | Kept for reference; not read by `report.py` |
| `results/v3-split/` | Test results on the v3 split | `v2__test.json`, `v3__test.json` | Kept for reference; not read by `report.py` |
| `results_v1_holdout.log`, `results_v1_test.log` | v1 evaluation console logs | Progress and summary lines | Reference |
| `post_training.log` | Post-training run log (v2) | Output of `make fuse`, `make gguf`, `make ollama`, and the v1/v2 evaluations | Record of the v2 export |
| `post_training_v3.log` | Post-training run log (v3) | v3 export, then the v3 hold-out/test and v2 test evaluations | Record of the v3 export |
| `post_training_v5.sh`, `post_training_v5.log` | v5 post-training chain and its log | Waits for training, exports v5, evaluates it on the hold-out, v4 hold-out, real queries and fixed test set | Record of the v5 export and evaluation |
| `post_training_v4.sh`, `post_training_v4.log` | v4 post-training chain and its log | Waits for training, exports v4, evaluates v4 on all sets and v3 on the new test sample | Record of the v4 export and evaluation |
| `post_training_v6.sh`, `post_training_v6.log` | v6 post-training chain and its log | Waits for training, exports v6, evaluates v6 across 4 benchmarks (--timeout 600), restarts Docker app | Automated v6 pipeline execution |

---

## 🏛️ Historical Archives (deleted)

`archive/` (~14 GB: the template-based v1 dataset, adapters, fused model, HF export and GGUF; the first fine-tune on the non-Coder `Qwen2.5-3B-Instruct`; a 3-example trial run) and the legacy `mlx-env/` venv were deleted on 2026-09-27. `sql2pyspark:v1` in Ollama keeps its own copy of the v1 model.

Other folders: `llama.cpp/` (upstream checkout, used only for `convert_hf_to_gguf.py`). `sql2pyspark-hf/` (~5.8 GB) is only needed as input for `make gguf` and is recreated by `make fuse`.
