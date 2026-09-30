SELECT status, MONTH(start_date) AS start_month, AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
WHERE NOT (YEAR(start_date) >= 2023 AND customer_id IN (1, 8, 12, 15))
GROUP BY status, MONTH(start_date)
HAVING avg_monthly_fee < 150
