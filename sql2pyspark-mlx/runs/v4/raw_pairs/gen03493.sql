SELECT t.id, t.txn_time, t.is_flagged
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE account_type <> 'CREDIT');
