SELECT 'transaction' AS source, txn_type
FROM transactions
WHERE TO_DATE(txn_time) >= '2023-10-01'
UNION ALL
SELECT 'session' AS source, country AS txn_type
FROM sessions;
