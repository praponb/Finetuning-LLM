#!/bin/zsh
# v5 post-training chain: wait for training, export, evaluate v5 on the 4 eval sets. Log: eval/post_training_v5.log
# (v3 and v4 results on the same sets already exist, incl. the fixed test set eval/test_fixed.jsonl.)
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" sql2pyspark-mlx/train.log; do sleep 30; done
grep -q "^exit=0" sql2pyspark-mlx/train.log || { step "training failed; stopping"; exit 1; }
step "export v5"; make export TAG=v5 || { step "export failed"; exit 1; }
step "v5 holdout";      $PY eval/evaluate.py --model sql2pyspark:v5 --set eval/holdout.jsonl
step "v5 holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:v5 --set eval/holdout_v4.jsonl
step "v5 real_queries"; $PY eval/evaluate.py --model sql2pyspark:v5 --set eval/real_queries.jsonl --tables eval/real_tables.py
step "v5 test_fixed";   $PY eval/evaluate.py --model sql2pyspark:v5 --set eval/test_fixed.jsonl
step "ALL DONE"
