SELECT status, opened_date
FROM accounts
WHERE status = 'OPEN'
UNION
SELECT status, opened_date
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CHECKING');
