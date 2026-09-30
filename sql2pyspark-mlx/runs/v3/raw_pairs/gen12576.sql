SELECT status, 'order' AS source
FROM orders
WHERE discount >= 0.37
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions;
