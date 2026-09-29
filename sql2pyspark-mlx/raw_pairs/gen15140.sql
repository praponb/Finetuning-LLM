SELECT
    status,
    MAX(amount) AS max_amount,
    COUNT(*) AS cnt,
    MIN(paid_at) AS min_paid_at
FROM payments
GROUP BY status
HAVING COUNT(*) >= 4
ORDER BY 1 ASC
LIMIT 5
