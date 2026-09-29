SELECT
    txn_type,
    COUNT(*) AS num_transactions,
    ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada,
    MAX(amount) - MIN(amount) AS amount_range
FROM transactions
WHERE txn_time < '2024-04-01'
GROUP BY txn_type;
