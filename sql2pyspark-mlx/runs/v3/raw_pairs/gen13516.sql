SELECT id, paid_at, order_id, CONCAT(status, ' ', method) AS status_method FROM payments
