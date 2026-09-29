SELECT
    id,
    status,
    paid_at,
    amount,
    LAG(amount, 2) OVER (PARTITION BY status ORDER BY paid_at, id) prev_amount
FROM payments
