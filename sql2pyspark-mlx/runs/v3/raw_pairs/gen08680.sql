SELECT
    QUARTER(paid_at) AS paid_quarter,
    method,
    MAX(paid_at) AS last_paid_at,
    COUNT(amount) AS amount_count
FROM payments
GROUP BY QUARTER(paid_at), method
HAVING COUNT(amount) > 2
ORDER BY method
