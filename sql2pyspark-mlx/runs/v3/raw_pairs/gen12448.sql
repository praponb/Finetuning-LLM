SELECT t.id, t.account_id, t.txn_type
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE account_type NOT IN ('LOAN', 'CHECKING'))
