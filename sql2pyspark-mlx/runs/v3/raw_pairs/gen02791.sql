WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE opened_date <= '2025-07-01' AND account_type = 'SAVINGS'
),
accounts_stats AS (
    SELECT status, COUNT(*) AS total_count
    FROM recent_accounts
    GROUP BY status
)
SELECT status, total_count
FROM accounts_stats
ORDER BY total_count;
