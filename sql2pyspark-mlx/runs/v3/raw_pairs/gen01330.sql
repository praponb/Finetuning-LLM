SELECT t.id, t.txn_time, t.is_flagged, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id ORDER BY account_type
