SELECT
    id,
    status,
    amount,
    MIN(amount) OVER (PARTITION BY status) AS status_min_amount
FROM payments
WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS');
