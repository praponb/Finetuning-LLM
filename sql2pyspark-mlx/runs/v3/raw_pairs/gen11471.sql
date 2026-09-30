SELECT t.id, t.amount, t.txn_time FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NULL);
