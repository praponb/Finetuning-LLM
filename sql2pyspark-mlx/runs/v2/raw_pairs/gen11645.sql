SELECT
    txn_type,
    SUM(amount) total_amount,
    COUNT(*) cnt,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM transactions
GROUP BY txn_type
ORDER BY total_amount DESC, txn_type ASC
LIMIT 50;
