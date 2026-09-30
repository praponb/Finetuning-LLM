SELECT accounts.account_type, COUNT(transactions.id) AS transaction_count
FROM accounts
LEFT JOIN transactions ON accounts.id = transactions.account_id
GROUP BY accounts.account_type
