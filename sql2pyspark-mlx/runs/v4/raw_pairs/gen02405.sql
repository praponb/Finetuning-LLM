SELECT plan, end_date, CASE WHEN plan = 'standard' THEN 1 ELSE 0 END AS is_standard
FROM subscriptions
ORDER BY plan DESC
