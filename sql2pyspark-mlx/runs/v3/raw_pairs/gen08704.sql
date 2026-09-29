SELECT merchant, txn_type FROM transactions WHERE account_id IN (13, 21, 38) INTERSECT SELECT merchant, txn_type FROM transactions WHERE amount >= 2200;
