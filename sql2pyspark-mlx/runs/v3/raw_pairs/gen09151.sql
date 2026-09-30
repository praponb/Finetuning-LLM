SELECT status
FROM orders
WHERE shipping_fee < 37
UNION ALL
SELECT status
FROM subscriptions
ORDER BY status;
