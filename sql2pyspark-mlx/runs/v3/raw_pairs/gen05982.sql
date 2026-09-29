SELECT id, amount, txn_type FROM transactions WHERE amount >= (SELECT AVG(amount) FROM transactions);
