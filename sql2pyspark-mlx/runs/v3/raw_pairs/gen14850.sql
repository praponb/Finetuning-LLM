SELECT
    id,
    status,
    paid_at,
    amount,
    LEAD(amount, 2) OVER (PARTITION BY status ORDER BY paid_at, id) next_amount
FROM payments
WHERE status <> 'REFUNDED';
