#!/bin/zsh
# Extras after chain_q9f.sh (owner OK 11-24 ~05:40): runs when the q9f chain has ended.
#   1. q9fe-q4 = 50 / 50 average of fused q9f and fused q9e -> hold-out / v4 / fixed / dev (eval/results/variants/)
#      (skipped if q9f training did not finish)
#   2. q9de-q4 (built in step B) on the 12 sets (default result paths, as for q9e) + name slips
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: variants/step_extras.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
while pgrep -f "chain_q9f.sh" > /dev/null; do sleep 60; done
caffeinate -s -i -w $$ &
unload

# 1. q9fe
t=q9fe-q4
if grep -q "^\[pref\] finished" $P/train_q9f.log 2>/dev/null && [ -s $P/adapters_q9f/adapters.safetensors ]; then
  if [ ! -s $G/$t.gguf ]; then
    step "$t: fuse q9f + q9e -> average -> f16 -> Q4_K_M"
    fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse q9f failed; stopping"; exit 1; }
    fuse_adapter qwen35_pilot/adapters_q9e $W/hf_q9e || { step "fuse q9e failed; stopping"; exit 1; }
    rm -rf $W/hf_q9fe
    $PY $V/avg_weights.py $W/hf_q9fe $W/hf_q9f $W/hf_q9e || { step "avg failed; stopping"; exit 1; }
    rm -rf $W/hf_q9f $W/hf_q9e
    to_f16 $W/hf_q9fe $W/q9fe-f16.gguf || { step "f16 failed; stopping"; exit 1; }
    rm -rf $W/hf_q9fe
    quant $W/q9fe-f16.gguf $G/$t.gguf Q4_K_M || { step "quant failed; stopping"; exit 1; }
    rm -f $W/q9fe-f16.gguf
  fi
  tag $G/$t.gguf $t && smoke $t && quick_evals $t || step "$t: tag / smoke failed; skipping it"
else
  step "q9f training not finished; skipping q9fe"
fi

# 2. q9de-q4 on the 12 sets (hold-out / v4 / fixed were already scored in step B with the same flags: copied)
T=q9de-q4
for s in holdout holdout_v4 test_fixed; do
  [ -s eval/results/${T}__$s.json ] || cp $R/${T}__$s.json eval/results/${T}__$s.json
done
E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py
step "$T complex_v8 test (100 of 300)"
E --set eval/complex_v8_test.jsonl --tables $M/complex_v8/eval_tables.py --sample 100 --seed 0
step "$T IN / NOT IN held-out pairs (50)"
E --set $M/classic_v10/data/inlist_valid_meta.jsonl --out eval/results/inlist/${T}__inlist_valid.json
for v in 11 12 13 14; do
  step "$T targeted_v$v held-out pairs"
  E --set $M/targeted_v$v/data/heldout_meta.jsonl --out eval/results/v$v-heldout/${T}__heldout.json
done
step "$T schema-shift pairs (200)"
E --set $HS --tables eval/schema_shift_tables.py --out eval/results/schema-shift/${T}__schema_shift.json
step "$T complex schema-shift pairs (150 of 300)"
E --set $H --sample 150 --seed 0 --tables eval/complex_shift_tables.py --out eval/results/complex-shift/${T}__complex_shift.json
step "name slips"
if [ -s eval/results/q9f-q4__holdout.json ]; then python3 eval/name_slips.py --models v18a-q4 q9d-q4 q9de-q4 q9f-q4
else python3 eval/name_slips.py --models v18a-q4 q9d-q4 q9de-q4; fi
unload
step "EXTRAS DONE"
