SELECT is_flagged, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount, MIN(txn_time) AS first_txn_time
FROM transactions
WHERE account_id IN (1, 17, 20)
GROUP BY is_flagged
ORDER BY debit_amount DESC, first_txn_time, is_flagged
LIMIT 1
