SELECT
    id,
    status,
    paid_at,
    amount,
    FIRST_VALUE(amount) OVER (PARTITION BY status ORDER BY paid_at, id) first_amount
FROM payments
