SELECT c.id, c.email, COUNT(s.id) AS subscription_count
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE s.plan NOT IN ('standard', 'enterprise')
GROUP BY c.id, c.email
ORDER BY subscription_count;
