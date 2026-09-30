SELECT *
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT') AND (balance BETWEEN 57000 AND 61000 OR customer_id IS NOT NULL)
