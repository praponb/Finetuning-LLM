SELECT *, MONTH(end_date) AS end_date_month, YEAR(start_date) AS start_date_year
FROM subscriptions
ORDER BY end_date
