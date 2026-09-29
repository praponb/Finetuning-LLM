SELECT id, start_date
FROM subscriptions
WHERE plan = 'premium' AND monthly_fee >= 170
ORDER BY start_date
