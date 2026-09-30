SELECT status, 'account' AS source
FROM accounts
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions
WHERE status IN ('PAUSED', 'CANCELLED')
ORDER BY source
