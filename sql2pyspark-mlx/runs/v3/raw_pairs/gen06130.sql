SELECT txn_type, MIN(txn_time) AS first_txn_time
FROM transactions
GROUP BY txn_type
ORDER BY first_txn_time
