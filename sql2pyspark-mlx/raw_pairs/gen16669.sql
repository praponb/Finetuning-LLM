WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id = 11 AND status NOT IN ('CLOSED', 'OPEN') AND status != 'OPEN'
),
accounts_stats AS (
    SELECT account_type, COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) closed_count
    FROM valid_accounts
    GROUP BY account_type
)
SELECT *
FROM accounts_stats
WHERE closed_count >= 5
