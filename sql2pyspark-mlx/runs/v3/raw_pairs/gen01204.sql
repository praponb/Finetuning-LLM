SELECT *
FROM subscriptions
WHERE monthly_fee <= 120 AND plan IN ('premium', 'enterprise');
