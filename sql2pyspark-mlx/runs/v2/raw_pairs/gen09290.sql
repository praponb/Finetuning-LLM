SELECT status
FROM payments
WHERE amount < 1330
UNION ALL
SELECT status
FROM subscriptions;
