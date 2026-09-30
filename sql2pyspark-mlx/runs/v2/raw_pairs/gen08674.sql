SELECT id, monthly_fee, plan
FROM subscriptions
WHERE monthly_fee >= (SELECT MAX(monthly_fee) FROM subscriptions WHERE plan IN ('enterprise', 'standard'))
ORDER BY id, 2 DESC
LIMIT 3
