SELECT t.id, t.txn_time FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE status = 'CLOSED') ORDER BY t.txn_time DESC
