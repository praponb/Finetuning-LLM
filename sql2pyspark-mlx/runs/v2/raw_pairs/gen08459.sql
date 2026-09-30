SELECT txn_type, account_id FROM transactions WHERE account_id IN (1, 3, 25) INTERSECT SELECT txn_type, account_id FROM transactions WHERE is_flagged
