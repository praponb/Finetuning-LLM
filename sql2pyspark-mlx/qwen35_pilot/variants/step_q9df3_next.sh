#!/bin/zsh
# Follow-up to step_q9df3.sh (owner answers 11-25 ~12:45 before going away for a few hours):
#   - if BOTH q9df3 / q9df3b pass the quick gate: full evals on both (step_q9df3.sh does the best one; this does the other)
#   - if NEITHER passes: q9df3c-q4 = 0.5 q9d + 0.4375 q9f + 0.0625 q9f2 (1/16 of the fix), probe + quick evals + gate,
#     full evals if it passes
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: variants/step_q9df3_next.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
while kill -0 16539 2>/dev/null; do sleep 60; done
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
caffeinate -s -i -w $$ &
unload
full_evals() {
  local T=$1
  [ -s eval/results/complex-shift/${T}__complex_shift.json ] && { step "$T full evals already done"; return 0; }
  step "full evals: $T"
  for s in holdout holdout_v4 test_fixed; do
    [ -s eval/results/${T}__$s.json ] || cp $R/${T}__$s.json eval/results/${T}__$s.json
  done
  E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
  [ -s eval/results/${T}__real_queries.json ] || { step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py; }
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
  unload
  step "$T harness_suite (21)"
  $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 --num-predict 4096 --set eval/harness_suite.jsonl \
    --tables eval/harness_tables.py --out eval/results/harness/${T}__harness_suite.json
  $PY eval/harness_strict.py eval/results/harness/${T}__harness_suite.json
  unload
  step "$T real-style pairs (74)"
  $PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$T --think false
  unload
}
PASS=($(python3 $V/gate_q9df3.py --all q9df3-q4 q9df3b-q4))
step "passing q9df3 / q9df3b: ${PASS:-none}"
if [ ${#PASS} = 0 ]; then
  T=q9df3c-q4
  if [ ! -s $G/$T.gguf ]; then
    step "fuse q9d, q9f, q9f2"
    fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
    fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse q9f failed; stopping"; exit 1; }
    fuse_adapter qwen35_pilot/adapters_q9f2 $W/hf_q9f2 || { step "fuse q9f2 failed; stopping"; exit 1; }
    step "$T: weights 0.5 0.4375 0.0625 over q9d q9f q9f2 -> f16 -> Q4_K_M"
    rm -rf $W/hf_q9df3c
    $PY $V/avg_weights.py $W/hf_q9df3c $W/hf_q9d $W/hf_q9f $W/hf_q9f2 --weights 0.5 0.4375 0.0625 || { step "merge failed; stopping"; exit 1; }
    to_f16 $W/hf_q9df3c $W/q9df3c-f16.gguf || { step "f16 failed; stopping"; exit 1; }
    rm -rf $W/hf_q9df3c
    quant $W/q9df3c-f16.gguf $G/$T.gguf Q4_K_M || { step "quant failed; stopping"; exit 1; }
    rm -f $W/q9df3c-f16.gguf
    rm -rf $W/hf_q9d $W/hf_q9f $W/hf_q9f2   # my working folders
  fi
  tag $G/$T.gguf $T || { step "ollama create $T failed; stopping"; exit 1; }
  smoke $T || step "smoke $T failed"
  zsh $V/star_probe.sh $T
  quick_evals $T
  python3 $V/gate_q9df3.py v18a-q4 q9df-q4 q9df2h-q4 q9df3-q4 q9df3b-q4 $T
  PASS=($(python3 $V/gate_q9df3.py --all $T))
  [ ${#PASS} = 0 ] && { step "Q9DF3 NEXT DONE: q9df3c fails the quick gate too"; exit 0; }
fi
for t in $PASS; do full_evals $t; done
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9d-q4 q9df-q4 $PASS
step "Q9DF3 NEXT DONE (full evals: $PASS)"
