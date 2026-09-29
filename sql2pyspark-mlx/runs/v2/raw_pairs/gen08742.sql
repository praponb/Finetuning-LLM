SELECT id, order_id, method, paid_at, amount, LEAD(amount, 1) OVER (PARTITION BY order_id, method ORDER BY paid_at, id) AS next_amount FROM payments ORDER BY amount
