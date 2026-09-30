SELECT is_flagged, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count, ROUND((100 * SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell
FROM transactions
WHERE NOT (merchant = 'Lazada' AND amount > 3500)
GROUP BY is_flagged
ORDER BY pct_shell DESC, is_flagged
LIMIT 1
