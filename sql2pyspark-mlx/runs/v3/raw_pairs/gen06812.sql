SELECT txn_type, merchant, MAX(txn_time) AS latest_txn_time
FROM transactions
GROUP BY txn_type, merchant
ORDER BY latest_txn_time, merchant
