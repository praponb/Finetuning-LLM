SELECT id, txn_type FROM transactions WHERE txn_type != 'FEE' AND amount > 3000 AND amount BETWEEN 1100 AND 1200
