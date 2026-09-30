SELECT a.id, a.txn_type, a.amount, a.txn_time FROM transactions a WHERE a.amount = (SELECT MAX(b.amount) FROM transactions b WHERE b.txn_type = a.txn_type) ORDER BY a.id
