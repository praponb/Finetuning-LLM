SELECT plan, status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions GROUP BY plan, status;
