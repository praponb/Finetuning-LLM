SELECT id, order_id, status, CAST(amount AS INT) AS amount_int, CONCAT(status, ' ', method) AS status_method FROM payments WHERE paid_at < '2025-01-01';
