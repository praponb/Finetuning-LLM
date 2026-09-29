SELECT 'order' AS record_type, status
FROM orders
WHERE shipping_fee < 28
UNION ALL
SELECT 'subscription' AS record_type, status
FROM subscriptions
