SELECT t.id, t.txn_type, t.txn_time, t.merchant FROM transactions t WHERE t.txn_time < '2025-06-01' AND t.account_id IN (SELECT id FROM accounts WHERE account_type = 'SAVINGS');
