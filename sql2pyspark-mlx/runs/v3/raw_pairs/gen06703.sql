SELECT start_date, 'subscription' AS source
FROM subscriptions
UNION
SELECT opened_date, 'account' AS source
FROM accounts
