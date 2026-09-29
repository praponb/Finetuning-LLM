SELECT
    HOUR(txn_time) AS txn_hour,
    AVG(amount) AS avg_amount,
    ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada,
    COUNT(DISTINCT amount) AS unique_merchants
FROM transactions
GROUP BY 1;
