SELECT t.id, t.txn_time, t.merchant FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE account_type <> 'LOAN')
