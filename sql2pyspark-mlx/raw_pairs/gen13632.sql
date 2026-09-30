SELECT status, pct_credit, first_opened_date
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_credit, MIN(opened_date) first_opened_date
    FROM accounts
    GROUP BY status
) sub
WHERE pct_credit > 61
ORDER BY pct_credit DESC
