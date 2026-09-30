SELECT id, txn_type, txn_time, DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) dense_rnk FROM transactions;
