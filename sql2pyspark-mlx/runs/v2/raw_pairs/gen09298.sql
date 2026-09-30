SELECT status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions WHERE plan != 'enterprise' GROUP BY status;
