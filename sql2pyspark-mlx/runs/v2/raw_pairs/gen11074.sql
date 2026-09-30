SELECT transactions.id, transactions.merchant
FROM transactions
WHERE transactions.account_id IN (SELECT id FROM accounts WHERE status = 'OPEN')
ORDER BY transactions.merchant
