SELECT id, order_id, paid_at, method, FIRST_VALUE(method) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS first_method FROM payments WHERE paid_at < '2024-05-01';
