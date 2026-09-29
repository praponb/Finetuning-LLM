SELECT channel, status, COUNT(DISTINCT amount) AS unique_customers
FROM orders
GROUP BY 1, 2
ORDER BY status
