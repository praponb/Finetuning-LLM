WITH src AS (
    SELECT *, SUBSTRING(merchant, 1, 3) AS merchant_prefix
    FROM transactions
    WHERE account_id IN (13, 25, 29, 40)
),
transactions_stats AS (
    SELECT
        is_flagged,
        txn_type,
        MIN(txn_time) AS min_txn_time,
        COUNT(*) AS num_transactions
    FROM src
    GROUP BY is_flagged, txn_type
)
SELECT is_flagged, min_txn_time, num_transactions
FROM transactions_stats
ORDER BY num_transactions DESC, is_flagged;
