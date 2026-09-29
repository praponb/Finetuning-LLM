SELECT
    id,
    method,
    paid_at,
    amount,
    amount - LAG(amount, 1, 0) OVER (PARTITION BY method ORDER BY paid_at, id) AS amount_change
FROM payments
WHERE order_id = 18
