SELECT start_date AS event_date, 'subscription' AS source FROM subscriptions WHERE customer_id IS NULL UNION ALL SELECT signup_date AS event_date, 'customer' AS source FROM customers ORDER BY source
