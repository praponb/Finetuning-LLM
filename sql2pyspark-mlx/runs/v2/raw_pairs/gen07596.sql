SELECT t.is_flagged, t.txn_time, a.balance, a.account_type, a.customer_id FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id
