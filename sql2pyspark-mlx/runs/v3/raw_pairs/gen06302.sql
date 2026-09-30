SELECT launch_date, 'product' AS source FROM products WHERE price <= 690 UNION ALL SELECT end_date, 'subscription' AS source FROM subscriptions WHERE MONTH(start_date) IN (6, 9, 12);
