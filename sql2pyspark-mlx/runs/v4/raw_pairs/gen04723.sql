SELECT id, txn_type, txn_time
FROM transactions
WHERE TO_DATE(txn_time) < '2024-08-01' AND (account_id IN (29, 34) OR TO_DATE(txn_time) < '2025-05-01')
ORDER BY txn_time DESC;
