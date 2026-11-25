#!/bin/zsh
# Step C of the 11-23 improvement plan: collect the model's own mistakes on the self-play pool, then build the
# preference pairs (data_q9f/). Usage: zsh step_c.sh <tag, e.g. q9e-q4>   Waits for step_b.sh. Resumable.
# Log: qwen35_pilot/selfplay/step_c.log
cd /Users/praponb/GitHub/Finetuning-LLM
export PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin JAVA_HOME=/opt/homebrew/opt/openjdk@17
M=sql2pyspark-mlx; S=$M/qwen35_pilot/selfplay; PY=$M/.venv/bin/python; TAG=${1:?tag}
step() { echo "[$(date +%H:%M:%S)] $*"; }
while pgrep -f "variants/step_[ab].sh" > /dev/null; do sleep 60; done
[ -s $S/pool.jsonl ] || { step "pool missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &
step "collect with sql2pyspark:$TAG"
until grep -q "COLLECT DONE" $S/collect_$TAG.log 2>/dev/null; do
  $PY $S/collect.py --model sql2pyspark:$TAG >> $S/collect_$TAG.log 2>&1 || { step "collect exited; retry in 60 s"; sleep 60; }
  n=$((${n:-0} + 1)); [ $n -gt 10 ] && { step "collect failed 10 times; stopping"; exit 1; }
done
step "make preference pairs"
$PY $S/make_pref.py --raw $S/raw_$TAG.jsonl || { step "make_pref failed; stopping"; exit 1; }
for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done
step "STEP C DONE"
