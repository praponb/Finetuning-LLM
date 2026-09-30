SELECT DISTINCT segment
FROM customers
WHERE credit_limit <= 9800
ORDER BY segment DESC
LIMIT 5
