WITH RecentTransactions AS (
    SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE txn_type <> 'DEBIT'
),
agg AS (
    SELECT txn_type, MAX(txn_time) AS latest_txn_time, ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab
    FROM RecentTransactions
    GROUP BY txn_type
)
SELECT *
FROM agg
ORDER BY latest_txn_time, txn_type DESC;
