SELECT t.id, t.txn_type, t.txn_time FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id = 3) ORDER BY t.txn_time DESC;
