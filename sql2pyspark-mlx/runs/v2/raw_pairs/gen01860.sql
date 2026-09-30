SELECT accounts.id, accounts.account_type, accounts.balance FROM accounts WHERE accounts.id IN (SELECT account_id FROM transactions)
