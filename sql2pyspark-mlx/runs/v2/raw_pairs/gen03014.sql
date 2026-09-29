SELECT status
FROM subscriptions
WHERE plan IN ('premium', 'standard')
UNION
SELECT status
FROM subscriptions
WHERE monthly_fee < 120;
