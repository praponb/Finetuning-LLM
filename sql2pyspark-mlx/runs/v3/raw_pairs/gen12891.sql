SELECT id, paid_at, status, order_id, LENGTH(status) AS status_length FROM payments WHERE paid_at >= '2024-05-01'
