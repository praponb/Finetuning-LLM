SELECT status, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee FROM subscriptions WHERE monthly_fee > 70 GROUP BY status
