SELECT transactions.id, transactions.merchant
FROM transactions
WHERE transactions.account_id NOT IN (SELECT id FROM accounts WHERE status = 'OPEN')
ORDER BY merchant, transactions.id DESC;
