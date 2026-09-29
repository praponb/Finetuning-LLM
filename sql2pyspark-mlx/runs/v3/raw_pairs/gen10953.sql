SELECT accounts.id, accounts.account_type, accounts.status FROM accounts WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id) ORDER BY account_type DESC
