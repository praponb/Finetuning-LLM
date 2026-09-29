SELECT id, account_id, txn_type FROM transactions WHERE is_flagged = TRUE EXCEPT SELECT id, account_id, txn_type FROM transactions WHERE amount BETWEEN 1200 AND 2500;
