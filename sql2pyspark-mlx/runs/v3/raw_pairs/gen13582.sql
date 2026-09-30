SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE plan IN ('basic', 'standard') AND customer_id IN (12, 13)
GROUP BY 1;
