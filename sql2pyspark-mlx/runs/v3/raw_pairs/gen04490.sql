SELECT id, status, customer_id
FROM accounts
WHERE customer_id IS NOT NULL
UNION ALL
SELECT id, status, customer_id
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CREDIT', 'SAVINGS')
