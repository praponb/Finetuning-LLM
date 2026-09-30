SELECT launch_date AS event_date FROM products WHERE launch_date < '2024-10-01' UNION ALL SELECT start_date AS event_date FROM subscriptions ORDER BY event_date DESC
