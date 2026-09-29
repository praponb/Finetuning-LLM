SELECT end_date AS event_date, 'subscription' AS source
FROM subscriptions
UNION ALL
SELECT launch_date AS event_date, 'product' AS source
FROM products
