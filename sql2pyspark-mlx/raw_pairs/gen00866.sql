SELECT customers.country, COUNT(*) AS subscription_count
FROM subscriptions
JOIN customers ON subscriptions.customer_id = customers.id
WHERE subscriptions.plan IN ('enterprise', 'basic', 'premium')
GROUP BY customers.country;
