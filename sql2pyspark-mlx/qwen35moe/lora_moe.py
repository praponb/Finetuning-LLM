"""mlx_lm.lora for Qwen3.5-35B-A3B on the 32 GB Mac mini: the Qwen3.5 training patches from qwen35_pilot (chunked
WY-form Gated DeltaNet in training mode + chunked lm_head / masked cross-entropy loss) with a memory limit that fits a
~19.5 GB 4-bit base. Same arguments as mlx_lm.lora; batch_size must be 1.

Env: Q35_MEM_GB (MLX memory limit, default 25), Q35_CACHE_MB (free-buffer cache, default 512).
Usage (from sql2pyspark-mlx/):  .venv/bin/python qwen35moe/lora_moe.py -c qwen35moe/memtest.yaml
"""
import functools
import os
import sys
from pathlib import Path

os.environ.pop("MLX_MAX_OPS_PER_BUFFER", None)     # default Metal buffers (small ones tripped the watchdog on Qwen3.5)
os.environ.pop("MLX_MAX_MB_PER_BUFFER", None)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "qwen35_pilot"))
import gd_chunked  # noqa: E402
from chunked_loss import chunked_loss  # noqa: E402

gd_chunked.install()
import mlx.core as mx  # noqa: E402
import mlx_lm.lora as lora  # noqa: E402
from mlx_lm.models import qwen3_next  # noqa: E402


def _moe_call(self, x):
    """Qwen3NextSparseMoeBlock.__call__ (mlx-lm 0.31.3) with stop_gradient on the top-k expert indices, as the other
    mlx-lm MoE models have. Without it, LoRA on earlier layers makes the backward pass try to differentiate
    take_along_axis w.r.t. the integer indices: "[gather_axis] Cannot calculate VJP with respect to indices"."""
    gates = mx.softmax(self.gate(x), axis=-1, precise=True)
    k = self.top_k
    inds = mx.stop_gradient(mx.argpartition(gates, kth=-k, axis=-1)[..., -k:])
    scores = mx.take_along_axis(gates, inds, axis=-1)
    if self.norm_topk_prob:
        scores = scores / scores.sum(axis=-1, keepdims=True)
    y = (self.switch_mlp(x, inds) * scores[..., None]).sum(axis=-2)
    return y + mx.sigmoid(self.shared_expert_gate(x)) * self.shared_expert(x)


qwen3_next.Qwen3NextSparseMoeBlock.__call__ = _moe_call

mx.set_cache_limit(int(os.environ.get("Q35_CACHE_MB", "512")) << 20)
mx.set_memory_limit(int(float(os.environ.get("Q35_MEM_GB", "25")) * (1 << 30)))

lora.train = functools.partial(lora.train, loss=chunked_loss)
lora.main()
