SELECT txn_type, COUNT(*) AS transaction_count
FROM transactions
GROUP BY txn_type
ORDER BY transaction_count DESC, 1
LIMIT 20
