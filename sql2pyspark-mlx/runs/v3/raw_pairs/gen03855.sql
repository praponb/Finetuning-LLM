WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE MONTH(opened_date) IN (3, 4, 9)
),
status_summary AS (
    SELECT
        status,
        account_type,
        AVG(balance) AS avg_balance,
        SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance
    FROM valid_accounts
    GROUP BY status, account_type
)
SELECT *
FROM status_summary
WHERE open_balance >= 61
ORDER BY account_type
