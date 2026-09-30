SELECT ROUND((100 * SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_standard, MIN(monthly_fee) AS min_monthly_fee FROM subscriptions
