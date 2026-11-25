#!/bin/zsh
# After q9f (11-24 12:50): q9f-q4 reached hold-out 61 but fixed 135 (5 repetition loops). Weight interpolation keeps
# part of the ORPO change and usually removes such degeneration. Replaces step_extras.sh (owner OK for the extras).
#   1. q9df-q4 = 50 / 50 fused q9d + fused q9f  (q9d + half the ORPO change)
#      q9fe-q4 = 50 / 50 fused q9f + fused q9e  (half the ORPO change + the q9d/q9e merge that helped in step B)
#      -> hold-out / v4 / fixed / dev (eval/results/variants/)
#   2. best of q9f / q9df / q9fe: hold-out >= 61 first, then hold-out + v4 + fixed + dev (BEST_OVERRIDE=<tag> to force) -> rest of the 12 sets, harness 21,
#      real-style 74 (default result paths, as for q9e)
#   3. q9de-q4 (step B) -> rest of the 12 sets      4. name slips v18a / q9d / q9de / best
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: variants/step_merge.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
while pgrep -f "chain_q9f.sh" > /dev/null; do sleep 60; done
caffeinate -s -i -w $$ &
unload

# 1. merges
need=0; for t in q9df-q4 q9fe-q4; do [ -s $G/$t.gguf ] || need=1; done
if [ $need = 1 ]; then
  step "fuse q9d, q9f, q9e"
  fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse q9f failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9e $W/hf_q9e || { step "fuse q9e failed; stopping"; exit 1; }
fi
for pair in "q9df q9d q9f" "q9fe q9f q9e"; do
  set -- ${=pair}; name=$1; t=$1-q4
  if [ ! -s $G/$t.gguf ]; then
    step "$t: average $2 + $3 -> f16 -> Q4_K_M"
    rm -rf $W/hf_$name
    $PY $V/avg_weights.py $W/hf_$name $W/hf_$2 $W/hf_$3 || { step "avg $name failed; stopping"; exit 1; }
    to_f16 $W/hf_$name $W/$name-f16.gguf || { step "f16 $name failed; stopping"; exit 1; }
    rm -rf $W/hf_$name
    quant $W/$name-f16.gguf $G/$t.gguf Q4_K_M || { step "quant $t failed; stopping"; exit 1; }
    rm -f $W/$name-f16.gguf
  fi
done
rm -rf $W/hf_q9d $W/hf_q9f $W/hf_q9e   # my working folders
for t in q9df-q4 q9fe-q4; do
  tag $G/$t.gguf $t || { step "ollama create $t failed; stopping"; exit 1; }
  smoke $t || { step "smoke $t failed; skipping it"; continue; }
  quick_evals $t
done

# 2. pick the best
BEST=${BEST_OVERRIDE:-$($PY - <<'EOF'
import json, os
def n_ok(m, s):
    for p in (f"eval/results/variants/{m}__{s}.json", f"eval/results/{m}__{s}.json"):
        if os.path.exists(p):
            return sum(c["strict"]["match"] for c in json.load(open(p))["cases"])
    return None
rows = []
for m in ["q9f-q4", "q9df-q4", "q9fe-q4"]:
    sc = [n_ok(m, s) for s in ("holdout", "holdout_v4", "test_fixed", "dev_short")]
    if None not in sc:
        rows.append((sc[0] >= 61, sum(sc), sc[0], m))   # hold-out >= 61 (the rule) first, then the sum
print(max(rows)[3])
EOF
)}
step "best of q9f / q9df / q9fe: $BEST"

full_rest() {
  local T=$1
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
}
full_rest $BEST
unload
step "$BEST harness_suite (21)"
$PY eval/evaluate.py --model sql2pyspark:$BEST --think false --timeout 600 --num-predict 4096 --set eval/harness_suite.jsonl \
  --tables eval/harness_tables.py --out eval/results/harness/${BEST}__harness_suite.json
$PY eval/harness_strict.py eval/results/harness/${BEST}__harness_suite.json
unload
step "$BEST real-style pairs (74)"
$PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$BEST --think false
unload
step "BEST DONE ($BEST)"

# 3. q9de
full_rest q9de-q4
unload
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9d-q4 q9de-q4 $BEST
step "MERGE STEP DONE"
