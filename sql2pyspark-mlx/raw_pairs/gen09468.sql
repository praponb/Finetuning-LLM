SELECT opened_date, 'store' AS kind FROM stores UNION SELECT start_date AS opened_date, 'subscription' AS kind FROM subscriptions
