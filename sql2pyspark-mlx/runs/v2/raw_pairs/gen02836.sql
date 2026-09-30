SELECT opened_date, account_type
FROM accounts
WHERE customer_id = 24
UNION
SELECT opened_date, account_type
FROM accounts
WHERE customer_id = 30
ORDER BY opened_date
