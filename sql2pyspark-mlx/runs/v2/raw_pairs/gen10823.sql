SELECT status
FROM orders
UNION ALL
SELECT status
FROM subscriptions
WHERE customer_id = 38
ORDER BY status;
