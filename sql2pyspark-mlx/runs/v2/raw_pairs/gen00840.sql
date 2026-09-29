SELECT MONTH(start_date) start_month, MAX(monthly_fee) max_monthly_fee, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee
FROM subscriptions
GROUP BY 1
HAVING COUNT(CASE WHEN plan = 'standard' THEN 1 END) >= 5
ORDER BY start_month DESC
LIMIT 50;
