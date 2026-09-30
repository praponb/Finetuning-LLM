SELECT transactions.id, transactions.merchant, accounts.balance
FROM transactions
LEFT JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.amount BETWEEN 3300 AND 3900 AND accounts.status = 'OPEN'
ORDER BY accounts.balance DESC, id
LIMIT 20;
