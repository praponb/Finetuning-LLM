SELECT id, merchant, txn_time, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS txn_time_dense_rank FROM transactions;
