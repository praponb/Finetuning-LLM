SELECT id, order_id, paid_at, amount, LAG(amount, 2, 0) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_amount FROM payments WHERE status = 'REFUNDED';
