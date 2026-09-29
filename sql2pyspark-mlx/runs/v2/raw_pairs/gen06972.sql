SELECT subscriptions.id, subscriptions.monthly_fee
FROM subscriptions
WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE name LIKE 'bo%')
