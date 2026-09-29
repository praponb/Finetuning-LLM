WITH Src AS (
    SELECT *
    FROM transactions
    WHERE txn_type <> 'CREDIT' AND account_id IN (25, 34, 38, 40)
),
transactions_stats AS (
    SELECT merchant, MIN(txn_time) AS first_txn_time
    FROM Src
    GROUP BY merchant
)
SELECT merchant, first_txn_time
FROM transactions_stats
ORDER BY merchant;
