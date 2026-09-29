SELECT launch_date AS event_date FROM products UNION SELECT start_date AS event_date FROM subscriptions WHERE plan != 'enterprise';
