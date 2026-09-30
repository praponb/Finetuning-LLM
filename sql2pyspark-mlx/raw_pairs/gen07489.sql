SELECT id, amount, txn_type FROM transactions WHERE amount = (SELECT MIN(amount) FROM transactions);
