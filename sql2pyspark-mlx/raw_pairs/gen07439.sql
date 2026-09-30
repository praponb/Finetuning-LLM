WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE is_flagged AND amount >= 2100 AND is_flagged = TRUE
),
transactions_stats AS (
    SELECT merchant, HOUR(txn_time) AS txn_hour, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM valid_transactions
    GROUP BY merchant, HOUR(txn_time)
)
SELECT merchant, txn_hour, txn_type_list
FROM transactions_stats
ORDER BY merchant
