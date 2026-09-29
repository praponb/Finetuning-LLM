SELECT
    CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END AS amount_bucket,
    SUM(CASE WHEN merchant = 'Lazada' THEN amount ELSE 0 END) AS lazada_amount,
    MIN(txn_time) AS first_txn_time,
    MAX(txn_time) AS last_txn_time
FROM transactions
GROUP BY CASE WHEN amount >= 3300 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END
ORDER BY first_txn_time ASC
