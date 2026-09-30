SELECT t.id, t.txn_time, t.is_flagged, t.account_id FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE account_type = 'LOAN') ORDER BY id DESC;
