SELECT
    id,
    amount,
    method,
    HOUR(paid_at) AS paid_at_hour
FROM payments
WHERE amount <= 780
ORDER BY method ASC, id ASC
LIMIT 20
