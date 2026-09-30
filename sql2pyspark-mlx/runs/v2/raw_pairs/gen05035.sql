SELECT
    merchant,
    CASE WHEN amount >= 4100 THEN 'high' WHEN amount >= 1100 THEN 'mid' ELSE 'low' END AS amount_bucket,
    ROUND(AVG(amount), 2) AS avg_amount,
    MIN(txn_time) AS first_txn_time
FROM transactions
WHERE merchant IN ('Shell', 'Starbucks', 'Amazon')
GROUP BY merchant, amount_bucket
HAVING ROUND(AVG(amount), 2) < 32
ORDER BY first_txn_time, avg_amount;
