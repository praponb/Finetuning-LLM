SELECT t.id, t.account_id, t.txn_time FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE balance < 73000) ORDER BY t.id
