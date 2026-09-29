SELECT status, 'order' AS source
FROM orders
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
ORDER BY status
