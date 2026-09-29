SELECT status
FROM payments
UNION ALL
SELECT status
FROM subscriptions
WHERE customer_id IS NOT NULL;
