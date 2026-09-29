SELECT id, merchant FROM transactions INTERSECT ALL SELECT id, merchant FROM transactions WHERE HOUR(txn_time) BETWEEN 10 AND 15
