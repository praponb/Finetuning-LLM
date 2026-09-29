WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE status = 'OPEN' AND opened_date >= '2023-09-01'
),
agg AS (
    SELECT account_type, ROUND(AVG(balance), 2) AS avg_balance, MAX(opened_date) AS last_opened_date
    FROM valid_accounts
    GROUP BY account_type
)
SELECT *
FROM agg
WHERE avg_balance > 40
ORDER BY account_type, avg_balance
