SELECT ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee, AVG(monthly_fee) avg_monthly_fee FROM subscriptions WHERE status = 'PAUSED' ORDER BY stddev_monthly_fee DESC;
