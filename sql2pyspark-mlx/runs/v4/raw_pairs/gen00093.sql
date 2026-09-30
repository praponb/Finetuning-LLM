SELECT opened_date, 'account' source
FROM accounts
UNION ALL
SELECT end_date, 'subscription' source
FROM subscriptions
WHERE status = 'CANCELLED'
ORDER BY source DESC;
