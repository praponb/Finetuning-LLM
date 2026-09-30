SELECT
    id,
    status,
    paid_at,
    amount,
    SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id) AS running_amount
FROM payments
