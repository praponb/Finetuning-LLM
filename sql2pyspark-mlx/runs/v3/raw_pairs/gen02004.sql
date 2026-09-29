SELECT status
FROM payments
UNION
SELECT status
FROM subscriptions;
