SELECT c.segment, ROUND((100 * SUM(CASE WHEN s.plan = 'basic' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_basic
FROM subscriptions s
RIGHT JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
HAVING ROUND((100 * SUM(CASE WHEN s.plan = 'basic' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 49
ORDER BY segment DESC
