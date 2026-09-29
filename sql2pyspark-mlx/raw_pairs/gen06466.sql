SELECT id, status, order_id, paid_at, amount, LEAD(amount, 1) OVER (PARTITION BY status, order_id ORDER BY paid_at, id) AS next_amount FROM payments WHERE order_id IS NOT NULL ORDER BY 5 DESC;
