SELECT
    id,
    method,
    status,
    paid_at,
    amount,
    LAG(amount, 1) OVER (PARTITION BY method, status ORDER BY paid_at, id) AS prev_amount
FROM payments
WHERE amount > 1450
