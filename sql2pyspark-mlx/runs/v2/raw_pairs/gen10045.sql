SELECT txn_type FROM transactions WHERE HOUR(txn_time) BETWEEN 12 AND 16 EXCEPT SELECT txn_type FROM transactions WHERE amount BETWEEN 1600 AND 3200 ORDER BY txn_type DESC
