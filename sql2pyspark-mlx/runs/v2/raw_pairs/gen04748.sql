SELECT accounts.id, accounts.balance, accounts.opened_date
FROM accounts
WHERE accounts.id IN (SELECT account_id FROM transactions);
