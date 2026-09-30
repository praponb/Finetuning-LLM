SELECT accounts.id, accounts.customer_id
FROM accounts
WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
ORDER BY 2 DESC, id DESC;
