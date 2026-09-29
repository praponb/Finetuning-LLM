SELECT plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY plan
HAVING avg_monthly_fee < 39
ORDER BY avg_monthly_fee DESC
