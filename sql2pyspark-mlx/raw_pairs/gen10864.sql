SELECT
    DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month,
    CASE WHEN amount >= 4400 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END AS amount_bucket,
    MAX(txn_time) AS last_txn_time,
    SUM(amount) AS total_amount
FROM transactions
WHERE txn_type = 'DEBIT'
GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM'), CASE WHEN amount >= 4400 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END
