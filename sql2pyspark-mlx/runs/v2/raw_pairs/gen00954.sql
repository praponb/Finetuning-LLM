SELECT status, plan, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions GROUP BY status, plan ORDER BY stddev_monthly_fee DESC
