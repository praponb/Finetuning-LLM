SELECT t.id, t.txn_type, t.account_id, t.amount FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE status IS NULL);
