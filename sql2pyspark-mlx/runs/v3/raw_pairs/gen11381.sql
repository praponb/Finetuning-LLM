SELECT t.id, t.merchant, t.account_id, t.txn_time FROM transactions t WHERE t.merchant IS NULL AND t.account_id IN (SELECT id FROM accounts WHERE account_type != 'CREDIT');
