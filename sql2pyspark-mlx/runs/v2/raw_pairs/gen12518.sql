SELECT id, amount, txn_type, merchant FROM transactions WHERE amount > (SELECT MAX(amount) FROM transactions);
