SELECT start_date, plan, monthly_fee
FROM subscriptions
WHERE plan = 'enterprise' OR status = 'ACTIVE'
