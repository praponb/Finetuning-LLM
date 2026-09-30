SELECT subscriptions.id, subscriptions.end_date, subscriptions.monthly_fee
FROM subscriptions
WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE name IS NOT NULL)
ORDER BY subscriptions.end_date ASC;
