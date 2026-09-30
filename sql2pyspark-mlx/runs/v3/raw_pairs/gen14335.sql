SELECT status, pct_checking
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking
    FROM accounts
    WHERE balance BETWEEN 34000 AND 74000
    GROUP BY status
) AS t
WHERE pct_checking > 71
