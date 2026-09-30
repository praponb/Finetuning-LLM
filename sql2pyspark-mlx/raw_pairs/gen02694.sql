SELECT transactions.id, transactions.merchant, accounts.account_type
FROM transactions
INNER JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.customer_id IN (3, 6, 12, 31)
ORDER BY id DESC;
