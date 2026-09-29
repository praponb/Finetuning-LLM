SELECT amount, 'order' AS source
FROM orders
WHERE customer_id = 18
UNION
SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
