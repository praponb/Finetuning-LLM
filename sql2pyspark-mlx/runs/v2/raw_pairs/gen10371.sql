SELECT t.id, t.txn_type, t.txn_time FROM transactions t WHERE t.is_flagged AND t.account_id IN (SELECT id FROM accounts WHERE customer_id IS NULL) ORDER BY txn_time DESC;
