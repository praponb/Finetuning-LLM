SELECT 'subscription' AS source, status AS label
FROM subscriptions
WHERE customer_id IN (21, 27, 32, 37)
UNION ALL
SELECT 'store' AS source, store_name AS label
FROM stores
ORDER BY label DESC;
