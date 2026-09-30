SELECT channel, ROUND((100 * SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile, COUNT(DISTINCT discount) AS unique_stores
FROM orders
WHERE status <> 'PENDING' AND discount <= 0.1 AND discount BETWEEN 0.28 AND 0.36
GROUP BY 1
HAVING COUNT(DISTINCT discount) >= 5
ORDER BY channel DESC;
