SELECT t.id, t.merchant, t.account_id, t.is_flagged FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE account_type IN ('LOAN', 'CHECKING'))
