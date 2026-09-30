SELECT
    method,
    status,
    COUNT(DISTINCT amount) AS unique_statuss,
    MAX(amount) AS max_amount
FROM payments
GROUP BY method, status
HAVING COUNT(DISTINCT amount) >= 4
ORDER BY unique_statuss, method, status
LIMIT 3
