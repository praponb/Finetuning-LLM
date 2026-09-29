SELECT
    CASE WHEN amount >= 3600 THEN 'high' WHEN amount >= 1300 THEN 'mid' ELSE 'low' END AS amount_bucket,
    txn_type,
    MAX(txn_time) AS latest_txn_time,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE is_flagged = TRUE AND merchant IN ('Lazada', '7-Eleven')
GROUP BY 1, 2
HAVING avg_amount >= 20;
