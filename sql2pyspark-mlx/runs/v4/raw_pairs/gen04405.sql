SELECT account_type, pct_closed
FROM (
    SELECT account_type, ROUND((100 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_closed
    FROM accounts
    WHERE account_type = 'SAVINGS'
    GROUP BY account_type
) AS sub
WHERE pct_closed < 77
