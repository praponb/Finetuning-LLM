SELECT segment, ROUND((100 * SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_consumer, COUNT(DISTINCT credit_limit) AS unique_citys
FROM customers
WHERE city IS NULL AND is_active = TRUE
GROUP BY segment
ORDER BY pct_consumer DESC;
