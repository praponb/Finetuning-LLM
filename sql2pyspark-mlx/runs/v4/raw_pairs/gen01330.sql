SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY 1, 2
HAVING avg_monthly_fee >= 65
ORDER BY plan
