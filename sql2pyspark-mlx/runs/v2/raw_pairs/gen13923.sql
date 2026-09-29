SELECT
    is_flagged,
    DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month,
    MAX(txn_time) AS last_txn_time,
    MAX(txn_time) AS latest_txn_time,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE txn_type = 'DEBIT'
GROUP BY is_flagged, txn_month
ORDER BY txn_month DESC, is_flagged ASC
LIMIT 1
