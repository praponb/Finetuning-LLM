SELECT id, opened_date, account_type
FROM accounts
WHERE balance BETWEEN 36000 AND 66000
UNION ALL
SELECT id, opened_date, account_type
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN')
ORDER BY id
