SELECT *
FROM (
    SELECT account_type, ROUND((100 * SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_frozen
    FROM accounts
    WHERE customer_id IS NOT NULL
    GROUP BY account_type
) AS x
WHERE pct_frozen >= 34;
