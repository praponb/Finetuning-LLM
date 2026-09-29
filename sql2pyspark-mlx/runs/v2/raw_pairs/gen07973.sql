SELECT start_date, plan
FROM subscriptions
WHERE monthly_fee < 210 AND plan = 'standard'
