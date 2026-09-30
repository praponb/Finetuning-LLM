SELECT merchant, SUM(amount) AS total_amount, MAX(txn_time) AS latest_txn_time FROM transactions WHERE TO_DATE(txn_time) < '2024-08-01' GROUP BY merchant
