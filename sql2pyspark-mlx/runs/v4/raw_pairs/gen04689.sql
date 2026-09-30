SELECT customer_id
FROM accounts
WHERE account_type = 'CREDIT'
UNION
SELECT customer_id
FROM accounts
WHERE account_type NOT IN ('CREDIT', 'LOAN');
