SELECT
    status,
    MIN(amount) AS min_amount,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY status
ORDER BY min_amount, status
LIMIT 10
