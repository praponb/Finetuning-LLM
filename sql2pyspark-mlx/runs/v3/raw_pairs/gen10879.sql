SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE YEAR(start_date) >= 2023 AND customer_id IN (10, 14, 18, 35)
GROUP BY status;
