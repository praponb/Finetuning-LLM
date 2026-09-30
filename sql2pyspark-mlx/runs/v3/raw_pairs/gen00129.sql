SELECT a.id, a.balance, a.status, a.opened_date FROM accounts a WHERE a.account_type <> 'SAVINGS' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
