SELECT c.country, COUNT(*) AS row_count, COUNT(CASE WHEN s.plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions s
LEFT JOIN customers c ON c.id = s.customer_id
GROUP BY c.country
HAVING enterprise_count >= 1;
