SELECT
    id,
    merchant,
    txn_time,
    RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS txn_time_rank
FROM transactions;
