SELECT YEAR(end_date) AS end_year, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY YEAR(end_date)
HAVING ROUND(STDDEV(monthly_fee), 2) >= 27
