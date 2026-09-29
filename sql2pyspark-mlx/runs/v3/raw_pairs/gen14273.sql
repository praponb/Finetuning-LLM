SELECT plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, MIN(end_date) AS first_end_date FROM subscriptions GROUP BY plan ORDER BY first_end_date
