SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee FROM subscriptions WHERE plan NOT IN ('standard', 'enterprise', 'basic') GROUP BY status HAVING ROUND(AVG(monthly_fee), 2) > 61
