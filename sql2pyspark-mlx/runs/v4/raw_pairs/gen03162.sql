SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE customer_id = 32)
PIVOT (SUM(monthly_fee) FOR plan IN ('basic', 'enterprise', 'standard'))
ORDER BY status
