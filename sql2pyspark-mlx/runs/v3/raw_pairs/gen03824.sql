WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE NOT is_flagged AND amount >= 1200
),
transactions_stats AS (
    SELECT txn_type, MIN(txn_time) AS first_txn_time
    FROM valid_transactions
    GROUP BY txn_type
)
SELECT *
FROM transactions_stats
ORDER BY first_txn_time;
