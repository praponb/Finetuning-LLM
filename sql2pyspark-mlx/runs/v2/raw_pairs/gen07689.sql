SELECT txn_type, merchant FROM transactions WHERE amount < 1100 INTERSECT SELECT txn_type, merchant FROM transactions WHERE HOUR(txn_time) BETWEEN 11 AND 19
