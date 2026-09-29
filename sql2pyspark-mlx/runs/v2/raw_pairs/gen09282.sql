SELECT accounts.customer_id, accounts.status, transactions.account_id
FROM accounts
LEFT JOIN transactions ON transactions.account_id = accounts.id
ORDER BY customer_id
