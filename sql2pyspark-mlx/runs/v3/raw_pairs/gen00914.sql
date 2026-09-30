SELECT opened_date
FROM accounts
WHERE customer_id = 32
UNION ALL
SELECT end_date
FROM subscriptions;
