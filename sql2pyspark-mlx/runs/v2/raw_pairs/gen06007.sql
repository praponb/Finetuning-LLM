SELECT a.id, a.txn_type, a.amount, a.is_flagged FROM transactions a WHERE a.amount = (SELECT MIN(b.amount) FROM transactions b WHERE b.txn_type = a.txn_type);
