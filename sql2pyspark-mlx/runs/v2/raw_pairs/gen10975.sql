SELECT accounts.id, accounts.customer_id, accounts.status
FROM accounts
WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.account_id = 12);
