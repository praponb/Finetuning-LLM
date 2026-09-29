SELECT is_flagged, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count
FROM transactions
GROUP BY is_flagged
