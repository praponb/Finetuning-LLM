SELECT start_date AS event_date FROM subscriptions UNION ALL SELECT signup_date AS event_date FROM customers WHERE LOWER(TRIM(email)) = 'henry@startup.io'
