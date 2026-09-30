SELECT id, amount, txn_type FROM transactions WHERE amount >= (SELECT MAX(amount) FROM transactions)
