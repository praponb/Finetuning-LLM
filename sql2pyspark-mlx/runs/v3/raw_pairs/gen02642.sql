SELECT
    MONTH(txn_time) AS txn_month,
    txn_type,
    MAX(amount) - MIN(amount) AS amount_range,
    MAX(txn_time) AS last_txn_time,
    ROUND((100 * SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fee
FROM transactions
GROUP BY MONTH(txn_time), txn_type
HAVING MAX(amount) - MIN(amount) < 49
ORDER BY 4 DESC;
