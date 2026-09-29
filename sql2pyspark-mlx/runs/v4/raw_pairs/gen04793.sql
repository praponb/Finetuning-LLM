WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id = 13 AND account_id IN (16, 17, 23)
),
transactions_stats AS (
    SELECT is_flagged, COUNT(DISTINCT amount) unique_accounts
    FROM recent_transactions
    GROUP BY is_flagged
)
SELECT is_flagged, unique_accounts
FROM transactions_stats
ORDER BY unique_accounts;
