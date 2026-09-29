SELECT id, account_type, opened_date
FROM accounts
WHERE MONTH(opened_date) IN (6, 10, 11)
INTERSECT
SELECT id, account_type, opened_date
FROM accounts
WHERE status NOT IN ('CLOSED', 'OPEN', 'FROZEN');
