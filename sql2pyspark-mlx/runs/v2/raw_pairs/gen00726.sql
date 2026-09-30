SELECT txn_type FROM transactions WHERE merchant IN ('Lazada', '7-Eleven', 'Grab') UNION SELECT txn_type FROM transactions WHERE TO_DATE(txn_time) >= '2024-10-01' ORDER BY txn_type
