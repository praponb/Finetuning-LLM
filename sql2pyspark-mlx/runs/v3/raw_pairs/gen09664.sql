SELECT
    status,
    method,
    MAX(amount) AS max_amount,
    COUNT(*) AS payment_count
FROM payments
GROUP BY CUBE(status, method)
ORDER BY payment_count DESC
