#!/bin/zsh
# v4 post-training chain: export, then evaluate v4 (and v3 on the new test split). Log: eval/post_training_v4.log
# (The test sample is 150 queries: v4-split queries take ~20 s each, so 300 per model would take ~2 h.)
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" sql2pyspark-mlx/train.log; do sleep 30; done
grep -q "^exit=0" sql2pyspark-mlx/train.log || { step "training failed; stopping"; exit 1; }
step "export v4"; make export TAG=v4 || { step "export failed"; exit 1; }
step "move v3-split test results"; mkdir -p eval/results/v3-split && git mv -k eval/results/v2__test.json eval/results/v3__test.json eval/results/v3-split/ 2>/dev/null || mv eval/results/v2__test.json eval/results/v3__test.json eval/results/v3-split/
step "v4 holdout";      $PY eval/evaluate.py --model sql2pyspark:v4 --set eval/holdout.jsonl
step "v4 holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:v4 --set eval/holdout_v4.jsonl
step "v4 real_queries"; $PY eval/evaluate.py --model sql2pyspark:v4 --set eval/real_queries.jsonl --tables eval/real_tables.py
step "v4 test (v4 split)"; $PY eval/evaluate.py --model sql2pyspark:v4 --set sql2pyspark-mlx/data/test_meta.jsonl --sample 150
step "v3 test (v4 split)"; $PY eval/evaluate.py --model sql2pyspark:v3 --set sql2pyspark-mlx/data/test_meta.jsonl --sample 150
step "ALL DONE"
