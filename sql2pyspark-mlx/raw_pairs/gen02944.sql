SELECT
    txn_type,
    txn_day,
    unique_txn_types,
    first_txn_time
FROM (
    SELECT
        txn_type,
        TO_DATE(txn_time) AS txn_day,
        COUNT(DISTINCT amount) AS unique_txn_types,
        MIN(txn_time) AS first_txn_time
    FROM transactions
    GROUP BY txn_type, TO_DATE(txn_time)
) AS t
WHERE unique_txn_types > 5
