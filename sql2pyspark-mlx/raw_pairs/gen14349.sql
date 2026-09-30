WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE account_type = 'SAVINGS'
),
accounts_stats AS (
    SELECT account_type, MAX(opened_date) AS latest_opened_date
    FROM valid_accounts
    GROUP BY account_type
)
SELECT account_type, latest_opened_date
FROM accounts_stats
ORDER BY latest_opened_date DESC, account_type
LIMIT 3;
