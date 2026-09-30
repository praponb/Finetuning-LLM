SELECT account_id, MAX(txn_time) AS last_txn_time, ROUND((100 * SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_amazon
FROM transactions
GROUP BY 1
ORDER BY 3
