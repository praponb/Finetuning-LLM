"""Combine same-shape LoRA adapters into ONE adapter whose change is exactly the weighted sum of theirs (q9g step 0).

mlx-lm LoRA: y = x W + scale * (x @ lora_a) @ lora_b. For adapters i with the same scale, layers and modules,
    sum_i w_i * scale * (x A_i) B_i  =  scale * (x [A_1 | A_2 | ...]) [w_1 B_1 ; w_2 B_2 ; ...]
so concatenating the A's along the rank axis and the weighted B's along the rank axis gives a rank-(sum r_i) adapter
equal to the weighted merge (q9df3 = 0.5 q9d + 0.25 q9f + 0.25 q9f2 on the same 4-bit base). Dropout acts on x before
lora_a, so it is the same for all parts. Computed in float32, saved in the inputs' dtype.

  .venv/bin/python qwen35_pilot/q9g/combine_adapters.py OUT_DIR IN1 IN2 IN3 --weights 0.5 0.25 0.25
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import mlx.core as mx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("ins", nargs="+")
    ap.add_argument("--weights", nargs="+", type=float, required=True)
    a = ap.parse_args()
    ins = [Path(p) for p in a.ins]
    assert len(ins) == len(a.weights)
    cfgs = [json.loads((d / "adapter_config.json").read_text()) for d in ins]
    keys = ("num_layers", "fine_tune_type", "model")
    for c in cfgs[1:]:
        assert all(c[k] == cfgs[0][k] for k in keys), "adapter configs differ"
        assert c["lora_parameters"]["scale"] == cfgs[0]["lora_parameters"]["scale"], "scales differ"
    ws = [mx.load(str(d / "adapters.safetensors")) for d in ins]
    names = sorted(ws[0])
    assert all(sorted(w) == names for w in ws[1:]), "module lists differ"
    out = {}
    for n in names:
        dt = ws[0][n].dtype
        if n.endswith("lora_a"):          # [in, r]
            out[n] = mx.concatenate([w[n].astype(mx.float32) for w in ws], axis=-1).astype(dt)
        elif n.endswith("lora_b"):        # [r, out]
            out[n] = mx.concatenate([wt * w[n].astype(mx.float32) for wt, w in zip(a.weights, ws)], axis=0).astype(dt)
        else:
            raise SystemExit(f"unexpected tensor {n}")
    od = Path(a.out)
    od.mkdir(parents=True, exist_ok=True)
    mx.save_safetensors(str(od / "adapters.safetensors"), out)
    cfg = dict(cfgs[0])
    rank = sum(c["lora_parameters"]["rank"] for c in cfgs)
    cfg["lora_parameters"] = {**cfgs[0]["lora_parameters"], "rank": rank}
    cfg["combined_from"] = [{"adapter": str(d), "weight": w} for d, w in zip(ins, a.weights)]
    (od / "adapter_config.json").write_text(json.dumps(cfg, indent=4))
    print(f"{len(names)} tensors, rank {rank} -> {od}")


if __name__ == "__main__":
    main()
