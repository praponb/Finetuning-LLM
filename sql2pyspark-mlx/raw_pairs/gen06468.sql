SELECT accounts.id, accounts.opened_date FROM accounts WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.is_flagged);
