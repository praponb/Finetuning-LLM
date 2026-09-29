SELECT t.id, t.txn_time, t.is_flagged, a.status, a.opened_date, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE a.balance < 34000 ORDER BY t.txn_time
