SELECT t.id, t.txn_time FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id IN (3, 17, 29));
