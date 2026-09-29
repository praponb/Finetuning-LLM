SELECT status, CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic, DATE_TRUNC('month', start_date) AS start_date_period
FROM subscriptions
WHERE plan = 'standard'
