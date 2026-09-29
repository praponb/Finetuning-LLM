SELECT MAX(txn_time) latest_txn_time, COUNT(amount) amount_count
FROM transactions
WHERE is_flagged AND txn_time < '2024-07-01' AND txn_type = 'REFUND'
ORDER BY latest_txn_time DESC
