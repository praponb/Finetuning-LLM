SELECT accounts.id, accounts.opened_date, accounts.balance
FROM accounts
WHERE accounts.id IN (SELECT account_id FROM transactions WHERE amount <= 3800);
