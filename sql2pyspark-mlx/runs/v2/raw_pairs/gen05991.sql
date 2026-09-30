SELECT
    is_flagged,
    CASE WHEN amount >= 3800 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END AS amount_bucket,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    COUNT(DISTINCT amount) AS unique_merchants
FROM transactions
WHERE HOUR(txn_time) BETWEEN 8 AND 13 AND txn_time >= '2025-10-01'
GROUP BY is_flagged, CASE WHEN amount >= 3800 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(amount), 2) >= 28
ORDER BY stddev_amount DESC
