SELECT id, monthly_fee
FROM subscriptions
WHERE monthly_fee > (SELECT AVG(monthly_fee) FROM subscriptions WHERE plan NOT IN ('premium', 'standard'));
