SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE monthly_fee > 230)
PIVOT (MAX(monthly_fee) FOR plan IN ('basic', 'standard'))
