SELECT start_date, 'subscription' AS source FROM subscriptions UNION SELECT signup_date, 'customer' AS source FROM customers;
