SELECT *
FROM (
    SELECT status, account_type, COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
    FROM accounts
    WHERE balance BETWEEN 45000 AND 62000
    GROUP BY status, account_type
) AS x
WHERE savings_count < 6
ORDER BY account_type, status
LIMIT 3;
