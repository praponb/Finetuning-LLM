WITH base AS (
    SELECT *
    FROM transactions
    WHERE txn_time < '2025-04-22' AND is_flagged
),
transactions_stats AS (
    SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM base
    GROUP BY DATE_TRUNC('MONTH', txn_time)
)
SELECT txn_month, txn_type_list
FROM transactions_stats
