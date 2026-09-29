SELECT DATE_TRUNC('MONTH', end_date) AS end_month, YEAR(end_date) AS end_year, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions GROUP BY 1, 2
