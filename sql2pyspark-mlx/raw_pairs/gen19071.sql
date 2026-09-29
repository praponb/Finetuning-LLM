SELECT accounts.id, accounts.opened_date, accounts.account_type
FROM accounts
WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
ORDER BY accounts.opened_date ASC, accounts.id DESC;
