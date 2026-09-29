SELECT AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
WHERE (monthly_fee < 140 AND status <> 'ACTIVE') OR plan IN ('enterprise', 'premium', 'basic')
