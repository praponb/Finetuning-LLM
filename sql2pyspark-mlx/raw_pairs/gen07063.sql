SELECT id, amount, txn_time, is_flagged FROM transactions WHERE amount <= (SELECT MIN(amount) FROM transactions)
