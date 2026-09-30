SELECT t.id, t.txn_time, t.is_flagged, t.amount, a.account_type, a.customer_id FROM transactions t JOIN accounts a ON a.id = t.account_id
