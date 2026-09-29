SELECT * FROM (SELECT plan, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions GROUP BY plan) AS sub WHERE stddev_monthly_fee > 68 ORDER BY stddev_monthly_fee, 1;
