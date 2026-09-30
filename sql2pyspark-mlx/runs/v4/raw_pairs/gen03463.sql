SELECT t.id, t.is_flagged, t.txn_time FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE balance >= 54000) ORDER BY t.id DESC
