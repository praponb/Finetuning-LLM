SELECT transactions.account_id, accounts.status
FROM transactions
INNER JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.merchant = 'Lazada';
