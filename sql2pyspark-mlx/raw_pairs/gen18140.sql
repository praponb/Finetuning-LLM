SELECT id, status, CAST(amount AS INT) amount_int FROM payments WHERE order_id = 6 AND status IN ('SUCCESS', 'REFUNDED', 'FAILED') ORDER BY amount_int DESC, id LIMIT 1
