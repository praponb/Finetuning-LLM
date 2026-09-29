SELECT
    txn_type,
    is_flagged,
    MIN(txn_time) AS min_txn_time,
    ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada,
    AVG(amount) AS avg_amount
FROM transactions
GROUP BY txn_type, is_flagged
HAVING COUNT(amount) >= 3;
