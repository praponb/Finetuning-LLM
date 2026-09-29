SELECT plan, avg_monthly_fee FROM (SELECT plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee FROM subscriptions GROUP BY plan) AS sub WHERE avg_monthly_fee >= 50
