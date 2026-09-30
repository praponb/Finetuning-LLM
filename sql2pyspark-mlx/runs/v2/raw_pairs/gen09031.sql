SELECT status, 'order' AS source
FROM orders
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions;
