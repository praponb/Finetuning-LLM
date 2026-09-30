SELECT txn_type, MAX(txn_time) AS last_txn_time FROM transactions WHERE amount <= 1900 OR txn_type NOT IN ('CREDIT', 'DEBIT') GROUP BY 1
