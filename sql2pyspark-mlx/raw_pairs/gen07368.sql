SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, is_flagged, ROUND((100 * SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refund
FROM transactions
WHERE NOT is_flagged
GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM'), is_flagged
HAVING ROUND((100 * SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 37
ORDER BY pct_refund DESC, txn_month ASC;
