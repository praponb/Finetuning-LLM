SELECT id, monthly_fee
FROM subscriptions
WHERE monthly_fee >= (SELECT MAX(monthly_fee) FROM subscriptions)
ORDER BY id DESC
