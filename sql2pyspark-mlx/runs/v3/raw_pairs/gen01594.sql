SELECT a.id, a.account_id, a.amount, a.txn_type FROM transactions a WHERE a.amount > (SELECT AVG(b.amount) FROM transactions b WHERE b.account_id = a.account_id) ORDER BY a.amount DESC
