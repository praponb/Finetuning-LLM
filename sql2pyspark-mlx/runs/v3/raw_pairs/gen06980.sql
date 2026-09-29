SELECT id, order_id, paid_at, amount, LAG(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_amount FROM payments WHERE paid_at < '2024-12-31';
