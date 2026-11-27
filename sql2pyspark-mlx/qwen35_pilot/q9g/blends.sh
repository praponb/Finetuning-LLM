#!/bin/zsh
# q9g follow-up (owner 11-27 ~05:45): after chain_q9g.sh, try two more blends of fused q9g with fused q9df3
# (q9g weight 0.35 and 0.65), quick gate each; full evals only on a blend that BEATS q9gm-q4:
# passes the gate, v4 >= 21 (q9gm has 20), and gate key (harness v2 + hold-out + v4 + fixed + dev) >= q9gm's.
# Never touches sql2pyspark:latest, the root GGUFs or the site. Log: qwen35_pilot/q9g/blends.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
ensure_ollama() { until ollama list >/dev/null 2>&1; do sleep 2; done }
while pgrep -f "q9g/chain_q9g.sh" > /dev/null; do sleep 60; done
caffeinate -s -i -w $$ &
[ -d $W/hf_q9g ] || { step "fused q9g missing; stopping"; exit 1; }
# mk <tag> <q9g weight>: avg_weights of fused q9g and fused q9df3 (weight 1 = q9g itself) -> Q4_K_M -> tag
mk() {
  local T=$1 w=$2 hf=$W/hf_$1
  if [ ! -s $G/$T.gguf ]; then
    step "$T: q9g weight $w (rest q9df3) -> f16 -> Q4_K_M"
    if [ "$w" = 1 ]; then hf=$W/hf_q9g; else
      rm -rf $hf; $PY $V/avg_weights.py $hf $W/hf_q9g $W/hf_q9df3 --weights $w $((1 - w)) || return 1; fi
    to_f16 $hf $W/$T-f16.gguf || return 1
    [ "$hf" = "$W/hf_q9g" ] || rm -rf $hf
    quant $W/$T-f16.gguf $G/$T.gguf Q4_K_M || return 1
    rm -f $W/$T-f16.gguf
  fi
  ensure_ollama; tag $G/$T.gguf ${T#sql2pyspark:} || return 1
}
# gate <tag>: smoke, star probe, quick evals, harness v2 (+ strict)
gate() {
  local T=$1
  smoke $T || step "smoke $T failed"
  zsh $V/star_probe.sh $T
  quick_evals $T
  if [ ! -s eval/results/harness/${T}__harness_suite_v2.strict.json ]; then
    step "$T harness v2 (50)"
    $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 --num-predict 4096 --set eval/harness_suite_v2.jsonl \
      --tables eval/harness_tables_v2.py --out eval/results/harness/${T}__harness_suite_v2.json > $R/${T}__harness_v2.log 2>&1
    $PY eval/harness_strict.py --v2 eval/results/harness/${T}__harness_suite_v2.json >> $R/${T}__harness_v2.log 2>&1
  fi
  unload
}
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
  E --num-predict 4096 --set eval/harness_suite.jsonl --tables eval/harness_tables.py --out eval/results/harness/${T}__harness_suite.json
  $PY eval/harness_strict.py eval/results/harness/${T}__harness_suite.json
  unload
  step "$T real-style pairs (74)"
  $PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$T --think false
  unload
}

BEAT=()
for w in 0.35 0.65; do
  T=q9gw${w#0.}-q4
  mk $T $w || { step "export $T failed"; continue; }
  gate $T
  python3 $V/gate_q9g.py v18a-q4 q9df3-q4 q9gm-q4 $T
  if python3 - $T <<'PY'
import sys; sys.path.insert(0, "sql2pyspark-mlx/qwen35_pilot/variants")
from gate_q9g import row, key, passes
a, b = row(sys.argv[1]), row("q9gm-q4")
sys.exit(0 if passes(a) and a["holdout_v4"] >= 21 and key(a) >= key(b) else 1)
PY
  then BEAT+=($T); step "$T beats q9gm-q4"; else step "$T does not beat q9gm-q4"; fi
done
for t in $BEAT; do full_evals $t; done
[ ${#BEAT} -gt 0 ] && { step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9df3-q4 q9gm-q4 $BEAT; }
python3 $V/gate_q9g.py v18a-q4 q9df3-q4 q9g-q4 q9gm-q4 q9gw35-q4 q9gw65-q4
step "BLENDS DONE (full evals: ${BEAT:-none})"
