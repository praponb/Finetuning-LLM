SELECT id, order_id, method, paid_at, amount, amount - LAG(amount, 1, 0) OVER (PARTITION BY order_id, method ORDER BY paid_at, id) AS amount_change FROM payments;
