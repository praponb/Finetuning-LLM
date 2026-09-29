SELECT transactions.txn_type, accounts.account_type
FROM transactions
JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.merchant = '7-Eleven'
