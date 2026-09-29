SELECT t.id, t.merchant, t.txn_time, t.account_id FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE account_type != 'SAVINGS');
