SELECT account_type, COUNT(*) AS cnt
FROM accounts
WHERE customer_id IS NOT NULL
GROUP BY account_type
ORDER BY cnt DESC, account_type
LIMIT 10
