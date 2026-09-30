WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN')
),
account_type_summary AS (
    SELECT account_type, COUNT(balance) AS balance_count, AVG(balance) AS avg_balance
    FROM valid_accounts
    GROUP BY account_type
)
SELECT account_type, balance_count, avg_balance
FROM account_type_summary
ORDER BY 2 DESC;
