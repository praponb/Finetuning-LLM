SELECT
    merchant,
    is_flagged,
    COUNT(DISTINCT amount) AS unique_merchants,
    SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) AS refund_count
FROM transactions
WHERE HOUR(txn_time) BETWEEN 8 AND 15
GROUP BY CUBE(merchant, is_flagged)
ORDER BY 4 DESC
