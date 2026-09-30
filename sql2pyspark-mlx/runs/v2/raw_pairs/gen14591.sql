SELECT account_id, txn_type FROM transactions WHERE account_id IN (10, 19, 28) UNION SELECT account_id, txn_type FROM transactions WHERE txn_time < '2023-06-25'
