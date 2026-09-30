SELECT
    id,
    amount,
    status,
    paid_at,
    ROUND(amount, 1) amount_rounded
FROM payments
ORDER BY amount DESC
