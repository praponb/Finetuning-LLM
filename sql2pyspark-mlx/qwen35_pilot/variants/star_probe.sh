#!/bin/zsh
# Loop probe (eval/star_probe.jsonl, 30 SELECT * + subquery queries) for the given tags; num_predict 1024 bounds loops.
#   zsh star_probe.sh v18a-q4 q9df-q4 ...   -> eval/results/variants/<tag>__star_probe.json
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
for t in "$@"; do
  [ -s $R/${t}__star_probe.json ] && continue
  th=(--think false); [[ $t == q9* ]] || th=()
  step "$t star_probe"
  $PY eval/evaluate.py --model sql2pyspark:$t "${th[@]}" --timeout 600 --num-predict 1024 --set eval/star_probe.jsonl \
    --out $R/${t}__star_probe.json > $R/${t}__star_probe.log 2>&1
done
unload
step "STAR PROBE DONE"
