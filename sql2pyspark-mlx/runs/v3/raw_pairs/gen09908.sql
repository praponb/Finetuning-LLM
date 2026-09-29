SELECT id, txn_type FROM transactions WHERE merchant = 'Shell' UNION SELECT id, txn_type FROM transactions WHERE merchant IN ('Amazon', 'Starbucks')
