SELECT status
FROM subscriptions
INTERSECT
SELECT merchant
FROM transactions;
