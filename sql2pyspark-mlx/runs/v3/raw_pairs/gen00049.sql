SELECT accounts.id, accounts.status FROM accounts WHERE accounts.id IN (SELECT account_id FROM transactions WHERE account_id IN (32, 37, 38)) ORDER BY id DESC LIMIT 5;
