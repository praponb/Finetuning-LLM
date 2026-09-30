SELECT
    status,
    method,
    AVG(amount) AS avg_amount,
    COUNT(DISTINCT amount) AS unique_statuss
FROM payments
WHERE amount >= 560
GROUP BY status, method
HAVING avg_amount < 1060
ORDER BY 3 DESC;
