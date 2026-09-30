SELECT plan, status, COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions GROUP BY plan, status
