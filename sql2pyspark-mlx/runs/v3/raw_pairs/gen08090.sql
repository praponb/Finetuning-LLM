SELECT plan, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions WHERE YEAR(end_date) >= 2024 GROUP BY plan ORDER BY 2
