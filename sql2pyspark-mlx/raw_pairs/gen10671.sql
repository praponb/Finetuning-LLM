SELECT DATE_TRUNC('MONTH', end_date) AS end_month, AVG(monthly_fee) AS avg_monthly_fee, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE customer_id = 29 AND (end_date < '2024-08-22' OR status = 'PAUSED')
GROUP BY end_month
ORDER BY end_month
LIMIT 50
