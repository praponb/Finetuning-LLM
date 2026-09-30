SELECT *
FROM accounts
WHERE status NOT IN ('CLOSED', 'OPEN') AND account_type IN ('CREDIT', 'LOAN', 'SAVINGS');
