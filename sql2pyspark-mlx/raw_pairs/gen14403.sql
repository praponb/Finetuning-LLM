SELECT signup_date AS event_date, 'customer' AS record_type FROM customers UNION ALL SELECT start_date AS event_date, 'subscription' AS record_type FROM subscriptions
