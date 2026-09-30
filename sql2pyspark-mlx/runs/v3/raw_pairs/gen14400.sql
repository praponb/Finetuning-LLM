SELECT status
FROM orders
UNION ALL
SELECT status
FROM subscriptions
WHERE customer_id IS NULL
