SELECT cost FROM products WHERE launch_date < '2023-12-01' UNION SELECT monthly_fee FROM subscriptions ORDER BY cost
