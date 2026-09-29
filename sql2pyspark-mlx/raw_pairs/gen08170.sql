SELECT opened_date AS event_date, 'account' AS kind
FROM accounts
WHERE customer_id IN (14, 23, 26)
UNION ALL
SELECT end_date AS event_date, 'subscription' AS kind
FROM subscriptions
ORDER BY kind
