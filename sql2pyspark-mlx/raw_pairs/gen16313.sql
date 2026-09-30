SELECT merchant, txn_type FROM transactions INTERSECT ALL SELECT merchant, txn_type FROM transactions WHERE account_id IN (1, 38)
