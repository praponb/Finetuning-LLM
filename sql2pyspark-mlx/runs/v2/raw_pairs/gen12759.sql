SELECT status, 'account' AS source
FROM accounts
WHERE status = 'OPEN'
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions;
