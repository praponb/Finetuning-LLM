SELECT ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
FROM subscriptions
