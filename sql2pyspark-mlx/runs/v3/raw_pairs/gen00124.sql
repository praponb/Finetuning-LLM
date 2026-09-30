SELECT id, txn_time, txn_type, FIRST_VALUE(txn_type) OVER (ORDER BY txn_time, id) AS first_txn_type FROM transactions
