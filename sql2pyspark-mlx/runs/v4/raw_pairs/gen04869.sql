SELECT
    id,
    method,
    paid_at,
    amount,
    LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_amount
FROM payments
