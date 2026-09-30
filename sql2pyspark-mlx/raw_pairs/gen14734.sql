SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY 1
HAVING ROUND(AVG(monthly_fee), 2) > 59
