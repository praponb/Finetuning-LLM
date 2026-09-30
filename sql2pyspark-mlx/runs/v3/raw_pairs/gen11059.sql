SELECT id, order_id, paid_at, RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS rnk FROM payments WHERE paid_at >= '2024-08-12';
