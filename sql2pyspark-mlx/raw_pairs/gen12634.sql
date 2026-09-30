SELECT id, amount, txn_time, txn_type FROM transactions WHERE amount >= (SELECT MAX(amount) FROM transactions WHERE is_flagged = TRUE)
