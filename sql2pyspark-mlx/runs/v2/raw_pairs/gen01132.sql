SELECT accounts.status, accounts.account_type, COUNT(DISTINCT transactions.amount) AS unique_accounts
FROM transactions
JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.opened_date > '2023-05-01'
GROUP BY accounts.status, accounts.account_type;
