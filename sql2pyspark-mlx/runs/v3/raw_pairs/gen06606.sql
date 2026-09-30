SELECT customer_id, DATE_TRUNC('year', start_date) AS start_date_period, CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic FROM subscriptions WHERE plan = 'enterprise'
