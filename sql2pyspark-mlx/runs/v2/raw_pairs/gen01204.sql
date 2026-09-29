SELECT id, paid_at, order_id, status, CAST(amount AS INT) AS amount_int, LOWER(status) AS status_lower FROM payments
