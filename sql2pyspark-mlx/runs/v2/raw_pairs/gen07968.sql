SELECT is_flagged, MIN(txn_time) AS min_txn_time
FROM transactions
WHERE TO_DATE(txn_time) >= '2024-02-01'
GROUP BY is_flagged
