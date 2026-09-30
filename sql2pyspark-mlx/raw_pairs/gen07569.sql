WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IS NULL AND (status = 'OPEN' OR account_type = 'LOAN')
),
accounts_stats AS (
    SELECT account_type, COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
    FROM filtered_accounts
    GROUP BY account_type
)
SELECT *
FROM accounts_stats
ORDER BY account_type;
