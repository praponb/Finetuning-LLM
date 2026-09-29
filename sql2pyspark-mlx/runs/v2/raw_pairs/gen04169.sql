SELECT t.id, t.account_id, t.txn_time, t.amount FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NULL);
