SELECT t.id, t.amount, t.txn_time, t.is_flagged, a.status, a.account_type, a.balance FROM transactions t INNER JOIN accounts a ON t.account_id = a.id;
