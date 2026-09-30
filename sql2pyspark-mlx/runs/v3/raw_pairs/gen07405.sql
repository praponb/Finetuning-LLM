SELECT id, account_id, CONCAT(txn_type, '/', merchant) AS txn_type_merchant
FROM transactions
WHERE merchant = 'Shell'
ORDER BY id
