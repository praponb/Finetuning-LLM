#!/bin/zsh
# After the q9d chain: score v18a-q4 / q9c-q4 / q9d-q4 on the 21-case harness set (eval/harness_suite.jsonl,
# tables eval/harness_tables.py), then the type-strict re-check (eval/harness_strict.py). Owner OK 2026-11-21.
# Stretch set only (not in the promotion rule). Never touches latest / the site / tags. Log: eval/results/harness/run.log
cd /Users/praponb/GitHub/Finetuning-LLM
export PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin JAVA_HOME=/opt/homebrew/opt/openjdk@17
PY=sql2pyspark-mlx/.venv/bin/python; R=eval/results/harness; mkdir -p $R
CL=sql2pyspark-mlx/qwen35_pilot/chain_q9d.log
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -qE "\] ALL DONE|\] .*stopping|\] .*failed|training DIVERGED" $CL; do sleep 60; done
caffeinate -s -i -w $$ &
step "q9d chain ended; harness eval starts"
files=()
for T in v18a-q4 q9c-q4 q9d-q4; do
  ollama list | grep -q "^sql2pyspark:$T " || { step "$T: no Ollama tag, skipped"; continue; }
  THINK=(); [[ $T == q9* ]] && THINK=(--think false)
  step "$T harness_suite (21)"
  $PY eval/evaluate.py --model sql2pyspark:$T $THINK --timeout 600 --num-predict 4096 \
      --set eval/harness_suite.jsonl --tables eval/harness_tables.py --out $R/${T}__harness_suite.json
  files+=($R/${T}__harness_suite.json)
  ollama stop sql2pyspark:$T
done
step "type-strict re-check"
$PY eval/harness_strict.py $files
step "HARNESS DONE"
