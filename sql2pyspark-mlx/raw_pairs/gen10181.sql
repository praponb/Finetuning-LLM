SELECT status, plan, COUNT(DISTINCT monthly_fee) AS unique_plans FROM subscriptions GROUP BY 1, 2 ORDER BY plan
