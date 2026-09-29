SELECT c.is_active, ROUND((100 * SUM(CASE WHEN s.plan = 'enterprise' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_enterprise, COUNT(CASE WHEN s.status = 'EXPIRED' THEN 1 END) AS expired_count
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
ORDER BY expired_count DESC, is_active
LIMIT 50;
