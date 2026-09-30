SELECT
    is_flagged,
    MONTH(txn_time) AS txn_month,
    MIN(txn_time) AS first_txn_time,
    COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE TO_DATE(txn_time) >= '2025-10-01' OR merchant IN ('Amazon', 'Starbucks')
GROUP BY is_flagged, MONTH(txn_time)
HAVING credit_count > 2
