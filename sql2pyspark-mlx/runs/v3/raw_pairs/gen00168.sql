SELECT subscriptions.id, subscriptions.monthly_fee
FROM subscriptions
WHERE subscriptions.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL);
