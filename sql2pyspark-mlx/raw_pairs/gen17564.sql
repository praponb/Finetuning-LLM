SELECT 'payment' AS record_type, status
FROM payments
UNION
SELECT 'subscription' AS record_type, status
FROM subscriptions
WHERE plan IN ('enterprise', 'standard', 'basic');
