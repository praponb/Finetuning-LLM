SELECT t.id, t.merchant, t.account_id FROM transactions t WHERE t.is_flagged AND t.account_id IN (SELECT id FROM accounts WHERE account_type = 'LOAN');
