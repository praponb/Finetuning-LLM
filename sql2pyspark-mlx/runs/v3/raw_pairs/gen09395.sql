SELECT
    id,
    txn_type,
    txn_time,
    RANK() OVER (PARTITION BY txn_type ORDER BY txn_time) txn_time_rank
FROM transactions
WHERE TO_DATE(txn_time) >= '2025-09-02';
