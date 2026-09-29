SELECT is_flagged, MAX(txn_time) AS last_txn_time, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY is_flagged
ORDER BY avg_amount DESC, is_flagged
LIMIT 50
