SELECT * FROM (SELECT customer_id, plan, monthly_fee FROM subscriptions) PIVOT (AVG(monthly_fee) FOR plan IN ('basic', 'premium', 'enterprise', 'standard'))
