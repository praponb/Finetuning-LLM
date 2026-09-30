SELECT t.id, t.account_id, t.txn_type, t.is_flagged FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NOT NULL);
