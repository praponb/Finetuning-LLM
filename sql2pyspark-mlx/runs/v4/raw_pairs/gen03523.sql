SELECT cost, 'product' AS source FROM products WHERE MONTH(launch_date) IN (2, 4, 9) UNION SELECT monthly_fee, 'subscription' AS source FROM subscriptions
