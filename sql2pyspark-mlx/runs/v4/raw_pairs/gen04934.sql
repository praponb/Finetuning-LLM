SELECT id, order_id, paid_at, amount, LEAD(amount, 2, 0) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_amount FROM payments
