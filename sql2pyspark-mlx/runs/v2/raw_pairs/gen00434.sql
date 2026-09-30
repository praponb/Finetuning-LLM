SELECT is_flagged, COUNT(*) AS cnt, COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count
FROM transactions
WHERE TO_DATE(txn_time) < '2024-08-01' AND is_flagged
GROUP BY 1
HAVING COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) > 3
ORDER BY cnt DESC
