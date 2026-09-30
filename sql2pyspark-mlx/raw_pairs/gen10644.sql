SELECT
    status,
    method,
    MIN(amount) AS min_amount,
    SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
    COUNT(DISTINCT amount) AS unique_methods
FROM payments
GROUP BY status, method
