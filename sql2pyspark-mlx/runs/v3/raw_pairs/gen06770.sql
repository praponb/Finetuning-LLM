SELECT id, opened_date, account_type
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN') OR balance BETWEEN 34000 AND 78000
ORDER BY 1 ASC;
