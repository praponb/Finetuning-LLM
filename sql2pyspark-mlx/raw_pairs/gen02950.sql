SELECT id, order_id, paid_at, method, CAST(amount AS INT) amount_int FROM payments WHERE status = 'REFUNDED' AND status NOT IN ('SUCCESS', 'FAILED') ORDER BY amount_int DESC, id LIMIT 10
