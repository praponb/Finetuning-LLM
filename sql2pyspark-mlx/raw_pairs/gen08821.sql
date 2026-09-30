SELECT plan, status, ROUND((100 * SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_standard, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions GROUP BY plan, status
