#!/bin/zsh
# Option 1 (owner 11-25 ~12:25): smaller share of the q9f2 loop fix, no training.
#   q9df3-q4  = 0.5 q9d + 0.25  q9f + 0.25  q9f2   (= q9df + 1/4 of the fix delta; q9df2h had 1/2, q9df 0)
#   q9df3b-q4 = 0.5 q9d + 0.375 q9f + 0.125 q9f2   (= q9df + 1/8 of the fix delta)
#   1. fuse q9d / q9f / q9f2, merge, f16 -> Q4_K_M, Ollama tags, smoke
#   2. star probe (30) for both, then hold-out / v4 / fixed / dev for both
#   3. gate (gate_q9df3.py): 0 loops, hold-out >= 61, v4 >= 20, fixed >= 145. NONE -> stop ("Q9DF3 DONE: none passes").
#   4. best passing variant -> the other 9 sets, harness 21, real-style 74, name slips (as step_merge.sh)
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: variants/step_q9df3.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
caffeinate -s -i -w $$ &
unload
TAGS=(q9df3-q4 q9df3b-q4)

# 1. merges
need=0; for t in $TAGS; do [ -s $G/$t.gguf ] || need=1; done
if [ $need = 1 ]; then
  step "fuse q9d, q9f, q9f2"
  fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse q9f failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9f2 $W/hf_q9f2 || { step "fuse q9f2 failed; stopping"; exit 1; }
  for spec in "q9df3:0.5 0.25 0.25" "q9df3b:0.5 0.375 0.125"; do
    name=${spec%%:*}; wts=${spec#*:}; t=$name-q4
    [ -s $G/$t.gguf ] && continue
    step "$t: weights $wts over q9d q9f q9f2 -> f16 -> Q4_K_M"
    rm -rf $W/hf_$name
    $PY $V/avg_weights.py $W/hf_$name $W/hf_q9d $W/hf_q9f $W/hf_q9f2 --weights ${=wts} || { step "merge $name failed; stopping"; exit 1; }
    to_f16 $W/hf_$name $W/$name-f16.gguf || { step "f16 $name failed; stopping"; exit 1; }
    rm -rf $W/hf_$name
    quant $W/$name-f16.gguf $G/$t.gguf Q4_K_M || { step "quant $t failed; stopping"; exit 1; }
    rm -f $W/$name-f16.gguf
  done
  rm -rf $W/hf_q9d $W/hf_q9f $W/hf_q9f2   # my working folders
fi
for t in $TAGS; do
  tag $G/$t.gguf $t || { step "ollama create $t failed; stopping"; exit 1; }
  smoke $t || step "smoke $t failed"
done

# 2. probe + quick evals
zsh $V/star_probe.sh $TAGS
for t in $TAGS; do quick_evals $t; done
python3 $V/gate_q9df3.py v18a-q4 q9df-q4 q9df2h-q4 $TAGS
step "Q9DF3 QUICK DONE"

# 3. gate
BEST=$(python3 $V/gate_q9df3.py --best $TAGS)
[ "$BEST" = NONE ] && { step "Q9DF3 DONE: none passes the quick gate"; exit 0; }
step "passes the quick gate: $BEST -> full evals"

# 4. full evals for the best (same as step_merge.sh)
T=$BEST
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
unload
step "$T harness_suite (21)"
$PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 --num-predict 4096 --set eval/harness_suite.jsonl \
  --tables eval/harness_tables.py --out eval/results/harness/${T}__harness_suite.json
$PY eval/harness_strict.py eval/results/harness/${T}__harness_suite.json
unload
step "$T real-style pairs (74)"
$PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$T --think false
unload
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9d-q4 q9df-q4 $T
step "Q9DF3 ALL DONE ($T)"
