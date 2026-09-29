SELECT id, amount, txn_type FROM transactions WHERE amount <= (SELECT MIN(amount) FROM transactions WHERE merchant = '7-Eleven') ORDER BY txn_type
