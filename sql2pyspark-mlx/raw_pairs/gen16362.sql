SELECT id, monthly_fee
FROM subscriptions
WHERE monthly_fee = (SELECT MIN(monthly_fee) FROM subscriptions);
