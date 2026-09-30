SELECT
    id,
    status,
    paid_at,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY status ORDER BY paid_at, id) next_amount
FROM payments
ORDER BY paid_at DESC
