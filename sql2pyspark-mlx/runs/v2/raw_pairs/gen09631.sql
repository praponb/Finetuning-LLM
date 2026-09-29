SELECT accounts.customer_id, accounts.balance, transactions.account_id FROM accounts INNER JOIN transactions ON transactions.account_id = accounts.id WHERE accounts.opened_date > '2024-04-29'
