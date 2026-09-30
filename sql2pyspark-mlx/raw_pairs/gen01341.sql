SELECT id, monthly_fee, plan
FROM subscriptions
WHERE monthly_fee >= (SELECT AVG(monthly_fee) FROM subscriptions);
