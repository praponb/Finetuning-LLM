SELECT status, COUNT(DISTINCT monthly_fee) AS unique_customers, COUNT(DISTINCT monthly_fee) AS unique_statuss
FROM subscriptions
WHERE NOT (plan IN ('basic', 'enterprise') OR plan = 'premium')
GROUP BY status
