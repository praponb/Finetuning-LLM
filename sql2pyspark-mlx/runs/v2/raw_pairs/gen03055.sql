SELECT t.id, t.txn_type, t.txn_time, a.status FROM transactions t INNER JOIN accounts a ON t.account_id = a.id ORDER BY status DESC;
