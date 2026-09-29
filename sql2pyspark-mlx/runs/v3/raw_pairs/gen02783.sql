SELECT t.txn_type, t.is_flagged, a.balance, a.account_type FROM transactions t INNER JOIN accounts a ON a.id = t.account_id WHERE a.status IS NULL ORDER BY a.account_type;
