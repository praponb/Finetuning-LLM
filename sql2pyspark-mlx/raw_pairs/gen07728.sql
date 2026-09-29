SELECT id, txn_time, DENSE_RANK() OVER (ORDER BY txn_time) AS txn_time_dense_rank
FROM transactions
