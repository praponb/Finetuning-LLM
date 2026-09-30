SELECT end_date, 'subscription' AS source
FROM subscriptions
WHERE plan NOT IN ('basic', 'standard', 'premium')
UNION ALL
SELECT launch_date AS end_date, 'product' AS source
FROM products
ORDER BY source
