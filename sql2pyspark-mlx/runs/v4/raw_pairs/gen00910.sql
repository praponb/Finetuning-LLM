SELECT customer_id, status FROM subscriptions WHERE plan = 'premium' UNION ALL SELECT customer_id, status FROM subscriptions WHERE plan NOT IN ('basic', 'enterprise')
