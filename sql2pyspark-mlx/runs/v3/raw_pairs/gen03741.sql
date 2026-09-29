SELECT ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions WHERE plan IN ('standard', 'premium', 'enterprise');
