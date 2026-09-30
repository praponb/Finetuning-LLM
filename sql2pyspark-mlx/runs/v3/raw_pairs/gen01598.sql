SELECT status, 'account' AS source
FROM accounts
WHERE customer_id = 9
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions
WHERE monthly_fee < 180
