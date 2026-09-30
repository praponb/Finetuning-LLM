SELECT status, paid_at, order_id, COALESCE(amount, 0) AS amount_filled FROM payments ORDER BY status DESC
