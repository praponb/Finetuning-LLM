WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE (balance >= 46000 AND balance BETWEEN 42000 AND 45000) OR status IN ('OPEN', 'FROZEN')
),
accounts_stats AS (
    SELECT status, SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count, AVG(balance) AS avg_balance
    FROM valid_accounts
    GROUP BY status
)
SELECT *
FROM accounts_stats
WHERE frozen_count >= 61;
