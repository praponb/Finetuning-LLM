WITH valid_transactions AS (
    SELECT *, LENGTH(merchant) AS merchant_length
    FROM transactions
    WHERE account_id = 23
),
txn_month_summary AS (
    SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, MIN(txn_time) AS first_txn_time, COUNT(DISTINCT amount) AS unique_txn_types
    FROM valid_transactions
    GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
)
SELECT txn_month, first_txn_time, unique_txn_types
FROM txn_month_summary
ORDER BY first_txn_time;
