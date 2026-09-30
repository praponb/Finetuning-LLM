SELECT status, pct_open
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_open
    FROM accounts
    WHERE balance BETWEEN 75000 AND 76000
    GROUP BY status
) AS t
WHERE pct_open < 27
ORDER BY pct_open DESC
