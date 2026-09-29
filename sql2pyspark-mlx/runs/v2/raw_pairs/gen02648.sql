SELECT a.id, a.txn_type, a.amount, a.account_id FROM transactions a WHERE a.amount = (SELECT MIN(b.amount) FROM transactions b WHERE b.txn_type = a.txn_type) ORDER BY a.txn_type, a.amount
