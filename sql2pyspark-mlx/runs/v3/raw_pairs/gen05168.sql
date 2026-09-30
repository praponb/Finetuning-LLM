SELECT
    status,
    MIN(amount) AS min_amount,
    MAX(paid_at) AS last_paid_at,
    COUNT(*) AS total_count
FROM payments
WHERE method IS NOT NULL
GROUP BY status
