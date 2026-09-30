SELECT * FROM (SELECT MONTH(end_date) AS end_month, status, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY MONTH(end_date), status) AS s WHERE total_monthly_fee >= 770
