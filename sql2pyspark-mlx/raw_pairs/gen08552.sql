WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE account_type IN ('CREDIT', 'SAVINGS', 'LOAN') AND opened_date <= '2025-04-01'
),
summary AS (
    SELECT account_type, MIN(opened_date) AS first_opened_date, ROUND((100 * SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_frozen
    FROM recent_accounts
    GROUP BY account_type
)
SELECT account_type, first_opened_date, pct_frozen
FROM summary
ORDER BY pct_frozen;
