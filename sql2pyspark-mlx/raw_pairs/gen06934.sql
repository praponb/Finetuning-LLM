SELECT s.*, x.txn_time, x.amount FROM accounts s LEFT JOIN LATERAL (SELECT c.txn_time, c.amount FROM transactions c WHERE c.account_id = s.id ORDER BY c.amount DESC, c.id DESC LIMIT 1) x ON TRUE
