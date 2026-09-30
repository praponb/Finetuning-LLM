SELECT SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions;
