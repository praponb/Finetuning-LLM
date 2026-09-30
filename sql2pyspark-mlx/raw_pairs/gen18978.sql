SELECT accounts.account_type, COUNT(transactions.id) transaction_count
FROM accounts
LEFT JOIN transactions ON accounts.id = transactions.account_id
WHERE accounts.customer_id = 11
GROUP BY accounts.account_type
ORDER BY account_type DESC, transaction_count
