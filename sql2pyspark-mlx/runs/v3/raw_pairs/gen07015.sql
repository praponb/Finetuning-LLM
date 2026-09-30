SELECT status, 'payment' AS source
FROM payments
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions;
