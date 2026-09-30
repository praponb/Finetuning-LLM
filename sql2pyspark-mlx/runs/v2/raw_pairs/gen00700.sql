SELECT status, 'account' source
FROM accounts
WHERE balance IS NULL
UNION ALL
SELECT status, 'subscription' source
FROM subscriptions
WHERE MONTH(end_date) IN (2, 6, 8);
