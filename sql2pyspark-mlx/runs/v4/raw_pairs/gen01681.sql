SELECT end_date, 'subscription' AS source FROM subscriptions WHERE customer_id IS NULL UNION ALL SELECT launch_date, 'product' AS source FROM products WHERE SIZE(tags) >= 2
