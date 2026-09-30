SELECT id, monthly_fee, status
FROM subscriptions
WHERE monthly_fee > (SELECT MAX(monthly_fee) FROM subscriptions WHERE monthly_fee IS NOT NULL)
