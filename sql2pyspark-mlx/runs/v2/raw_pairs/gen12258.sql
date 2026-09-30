SELECT
    CASE WHEN amount >= 3200 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END amount_bucket,
    TO_DATE(txn_time) txn_day,
    txn_type,
    ROUND(AVG(amount), 2) avg_amount,
    SUM(amount) total_amount
FROM transactions
WHERE HOUR(txn_time) BETWEEN 11 AND 15
GROUP BY CASE WHEN amount >= 3200 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END, TO_DATE(txn_time), txn_type
ORDER BY amount_bucket, avg_amount;
