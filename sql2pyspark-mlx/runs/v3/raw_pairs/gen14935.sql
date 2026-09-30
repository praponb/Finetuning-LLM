SELECT status, plan, MAX(monthly_fee) max_monthly_fee
FROM subscriptions
WHERE customer_id = 12 OR plan = 'enterprise'
GROUP BY status, plan;
