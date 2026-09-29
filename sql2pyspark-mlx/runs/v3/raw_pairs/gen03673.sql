SELECT plan, SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee FROM subscriptions WHERE start_date > '2024-09-01' GROUP BY plan ORDER BY plan;
