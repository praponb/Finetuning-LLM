SELECT account_type
FROM accounts
WHERE MONTH(opened_date) IN (1, 2, 8)
UNION
SELECT account_type
FROM accounts
WHERE status = 'CLOSED';
