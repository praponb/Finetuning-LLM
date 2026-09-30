SELECT subscriptions.id, subscriptions.customer_id, subscriptions.monthly_fee
FROM subscriptions
WHERE subscriptions.monthly_fee >= 220 AND subscriptions.customer_id IN (SELECT id FROM customers WHERE credit_limit > 8000);
