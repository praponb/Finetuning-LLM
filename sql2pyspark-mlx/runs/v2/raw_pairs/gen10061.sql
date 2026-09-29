SELECT MONTH(end_date) end_month, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee FROM subscriptions GROUP BY 1 ORDER BY end_month ASC;
