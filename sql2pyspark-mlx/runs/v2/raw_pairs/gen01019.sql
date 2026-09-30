SELECT accounts.id, accounts.status, accounts.balance FROM accounts WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
