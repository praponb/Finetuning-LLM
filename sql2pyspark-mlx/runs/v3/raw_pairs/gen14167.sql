SELECT DISTINCT merchant, txn_type
FROM transactions
ORDER BY merchant, txn_type
LIMIT 5;
