SELECT customers.country, SUM(CASE WHEN subscriptions.plan = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count, COUNT(CASE WHEN subscriptions.plan = 'premium' THEN 1 END) AS premium_count
FROM subscriptions
INNER JOIN customers ON customers.id = subscriptions.customer_id
WHERE subscriptions.status IN ('PAUSED', 'CANCELLED', 'EXPIRED')
GROUP BY customers.country
ORDER BY enterprise_count;
