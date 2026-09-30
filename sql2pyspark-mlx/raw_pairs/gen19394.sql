SELECT a.id, a.account_id, a.amount, a.txn_type FROM transactions a WHERE a.amount = (SELECT MAX(b.amount) FROM transactions b WHERE b.account_id = a.account_id) ORDER BY a.amount DESC
