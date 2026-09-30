SELECT DATE_TRUNC('MONTH', end_date) AS end_month, plan, MAX(start_date) AS latest_start_date
FROM subscriptions
GROUP BY CUBE(DATE_TRUNC('MONTH', end_date), plan)
ORDER BY end_month DESC
