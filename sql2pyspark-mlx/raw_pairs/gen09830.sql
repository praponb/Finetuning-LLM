SELECT 'subscription' AS source, plan
FROM subscriptions
WHERE start_date < '2023-10-03'
UNION
SELECT 'customer' AS source, email
FROM customers
WHERE is_active;
