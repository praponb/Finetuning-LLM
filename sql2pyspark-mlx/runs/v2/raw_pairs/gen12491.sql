WITH base AS (
    SELECT *
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2024-08-04' AND merchant IN ('Lazada', 'Amazon')
),
summary AS (
    SELECT merchant, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values, COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count
    FROM base
    GROUP BY merchant
)
SELECT merchant, txn_type_values, debit_count
FROM summary
