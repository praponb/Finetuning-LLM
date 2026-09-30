SELECT signup_date AS event_date FROM customers WHERE LOWER(email) LIKE '%vy@%' EXCEPT SELECT start_date AS event_date FROM subscriptions
