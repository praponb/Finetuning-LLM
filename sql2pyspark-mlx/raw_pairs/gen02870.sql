SELECT id, txn_type, merchant FROM transactions WHERE NOT (txn_type = 'DEBIT' AND amount <= 2100);
