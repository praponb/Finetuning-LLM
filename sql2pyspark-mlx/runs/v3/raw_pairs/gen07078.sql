SELECT
    is_flagged,
    CASE WHEN amount >= 3000 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END AS amount_bucket,
    COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count,
    MAX(amount) - MIN(amount) AS amount_range,
    MAX(amount) AS max_amount
FROM transactions
WHERE amount >= 3300 AND is_flagged = TRUE AND txn_time >= '2025-08-01'
GROUP BY ROLLUP(is_flagged, CASE WHEN amount >= 3000 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END);
