SELECT txn_type FROM transactions WHERE TO_DATE(txn_time) >= '2023-09-01' UNION SELECT txn_type FROM transactions WHERE account_id IN (5, 17, 29);
