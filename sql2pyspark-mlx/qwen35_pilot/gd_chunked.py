"""Chunked (WY-form) gated delta rule in MLX ops for Qwen3.5 LoRA training (pilot only).

mlx-lm 0.31.3 trains the Gated DeltaNet layers with a Python loop over the tokens (gated_delta_ops): ~10 tiny ops per
token per layer, ~8 MB of saved states per token, and so many small Metal command buffers that compiled steps trip the
macOS GPU watchdog. This is the chunked algorithm of transformers' torch_chunk_gated_delta_rule (Qwen3-Next /
Qwen3.5): inside a chunk of CHUNK tokens the recurrence becomes matrix products, and only one state per chunk is
carried, so autodiff gives a matmul-based backward with memory ~ T/CHUNK states.

Conventions follow mlx-lm's qwen3_5 layer: q and k arrive already normalised and scaled (no extra 1/sqrt(Dk)), q / k
have Hk heads and are repeated to Hv, the state is [B, Hv, Dv, Dk]. log_g = log of mlx-lm's compute_g.

install() replaces qwen3_5.gated_delta_update in training mode (use_kernel=False, no mask); inference is unchanged.
check() compares outputs and gradients with mlx-lm's reference ops.
"""
from __future__ import annotations

import mlx.core as mx
import mlx.nn as nn
from mlx_lm.models import gated_delta as gd
from mlx_lm.models import qwen3_5

CHUNK = 64


def _unit_lower_inv(m):
    """Inverse of unit lower-triangular matrices [..., n, n] (n a power of 2) by recursive block substitution."""
    n = m.shape[-1]
    if n == 1:
        return mx.ones_like(m)
    h = n // 2
    a_inv = _unit_lower_inv(m[..., :h, :h])
    b_inv = _unit_lower_inv(m[..., h:, h:])
    c = -(b_inv @ m[..., h:, :h] @ a_inv)
    top = mx.concatenate([a_inv, mx.zeros_like(m[..., :h, h:])], axis=-1)
    bottom = mx.concatenate([c, b_inv], axis=-1)
    return mx.concatenate([top, bottom], axis=-2)


def chunk_gated_delta(q, k, v, log_g, beta, state=None):
    """q, k: [B, T, Hk, Dk]; v: [B, T, Hv, Dv]; log_g, beta: [B, T, Hv]; state: [B, Hv, Dv, Dk] or None.
    Returns y [B, T, Hv, Dv] (dtype of q) and the final state [B, Hv, Dv, Dk] (float32)."""
    B, T, Hk, Dk = q.shape
    Hv, Dv = v.shape[-2:]
    if Hv // Hk > 1:
        q = mx.repeat(q, Hv // Hk, -2)
        k = mx.repeat(k, Hv // Hk, -2)
    out_dtype = q.dtype
    C = CHUNK
    pad = (C - T % C) % C
    # [B, H, T, D] float32, padded with zeros (beta 0 and log_g 0: padded tokens change nothing)
    q, k, v = (mx.pad(x.astype(mx.float32), [(0, 0), (0, pad), (0, 0), (0, 0)]).transpose(0, 2, 1, 3) for x in (q, k, v))
    beta, log_g = (mx.pad(x.astype(mx.float32), [(0, 0), (0, pad), (0, 0)]).transpose(0, 2, 1) for x in (beta, log_g))
    N = (T + pad) // C
    q, k, v = (x.reshape(B, Hv, N, C, -1) for x in (q, k, v))
    beta, log_g = (x.reshape(B, Hv, N, C) for x in (beta, log_g))
    k_beta = k * beta[..., None]
    v_beta = v * beta[..., None]

    gc = mx.cumsum(log_g, axis=-1)                                   # [B, H, N, C]
    tril = mx.tril(mx.ones((C, C), dtype=mx.bool_))
    strict = mx.tril(mx.ones((C, C), dtype=mx.bool_), k=-1)
    decay = mx.where(tril, mx.exp(mx.where(tril, gc[..., :, None] - gc[..., None, :], 0.0)), 0.0)   # [.., C, C]
    lower = mx.where(strict, (k_beta @ k.swapaxes(-1, -2)) * decay, 0.0)
    w = _unit_lower_inv(lower + mx.eye(C))                           # (I + L)^-1
    u = w @ v_beta                                                   # [B, H, N, C, Dv]
    k_cum = w @ (k_beta * mx.exp(gc)[..., None])                     # [B, H, N, C, Dk]

    s = mx.zeros((B, Hv, Dk, Dv), dtype=mx.float32) if state is None else state.astype(mx.float32).swapaxes(-1, -2)
    ys = []
    for i in range(N):
        qi, ki, gi = q[:, :, i], k[:, :, i], gc[:, :, i]
        attn = mx.where(tril, (qi @ ki.swapaxes(-1, -2)) * decay[:, :, i], 0.0)
        v_new = u[:, :, i] - k_cum[:, :, i] @ s
        ys.append((qi * mx.exp(gi)[..., None]) @ s + attn @ v_new)
        g_last = gi[..., -1:]
        s = s * mx.exp(g_last)[..., None] + (ki * mx.exp(g_last - gi)[..., None]).swapaxes(-1, -2) @ v_new
    y = mx.stack(ys, axis=2).reshape(B, Hv, N * C, Dv)[:, :, :T].transpose(0, 2, 1, 3)
    return y.astype(out_dtype), s.swapaxes(-1, -2)


def gated_delta_update(q, k, v, a, b, A_log, dt_bias, state=None, mask=None, use_kernel=True):
    if use_kernel or mask is not None:          # inference / padded batches: mlx-lm unchanged
        return gd.gated_delta_update(q, k, v, a, b, A_log, dt_bias, state, mask, use_kernel)
    beta = mx.sigmoid(b)
    log_g = -mx.exp(A_log.astype(mx.float32)) * nn.softplus(a + dt_bias)   # = log(compute_g(A_log, a, dt_bias))
    return chunk_gated_delta(q, k, v, log_g, beta, state)


def install():
    qwen3_5.gated_delta_update = gated_delta_update


def check(T=150, seed=0):
    """Outputs and input gradients vs mlx-lm's reference ops (Qwen3.5-9B head sizes, T not a multiple of CHUNK)."""
    mx.random.seed(seed)
    B, Hk, Hv, D = 1, 16, 32, 128
    q = (mx.fast.rms_norm(mx.random.normal((B, T, Hk, D)), None, 1e-6) * D ** -1.0).astype(mx.bfloat16)
    k = (mx.fast.rms_norm(mx.random.normal((B, T, Hk, D)), None, 1e-6) * D ** -0.5).astype(mx.bfloat16)
    v = mx.random.normal((B, T, Hv, D)).astype(mx.bfloat16)
    a = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    b = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    A_log = mx.random.normal((Hv,)) * 0.5
    dt_bias = mx.random.normal((Hv,)) * 0.5
    w = mx.random.normal((B, T, Hv, D))

    def loss(fn):
        def f(q, k, v, a, b):
            y, st = fn(q, k, v, a, b, A_log, dt_bias, None, None, False)
            return (y.astype(mx.float32) * w).sum() + st.sum()
        return f

    ref_val, ref_g = mx.value_and_grad(loss(gd.gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    new_val, new_g = mx.value_and_grad(loss(gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    print(f"loss ref {ref_val.item():.4f}  chunked {new_val.item():.4f}")
    for name, r, n in zip("qkvab", ref_g, new_g):
        r, n = r.astype(mx.float32), n.astype(mx.float32)
        rel = (mx.abs(r - n).max() / (mx.abs(r).max() + 1e-12)).item()
        print(f"grad {name}: max rel diff {rel:.2e}")


if __name__ == "__main__":
    check()
