SELECT status, 'order' source
FROM orders
UNION ALL
SELECT status, 'subscription' source
FROM subscriptions
ORDER BY source;
