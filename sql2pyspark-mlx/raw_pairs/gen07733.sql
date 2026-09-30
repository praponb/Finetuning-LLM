SELECT launch_date
FROM products
UNION ALL
SELECT end_date
FROM subscriptions
WHERE status = 'EXPIRED'
