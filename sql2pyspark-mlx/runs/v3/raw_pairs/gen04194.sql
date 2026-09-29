SELECT start_date, 'subscription' AS source FROM subscriptions UNION ALL SELECT launch_date, 'product' AS source FROM products ORDER BY source;
