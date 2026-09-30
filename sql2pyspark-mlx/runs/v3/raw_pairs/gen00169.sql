SELECT method, MIN(paid_at) AS min_paid_at
FROM payments
GROUP BY 1
HAVING COUNT(CASE WHEN method = 'card' THEN 1 END) < 3
ORDER BY 2, method;
