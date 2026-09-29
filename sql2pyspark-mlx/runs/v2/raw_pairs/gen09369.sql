SELECT
    TO_DATE(txn_time) txn_day,
    txn_type,
    COUNT(DISTINCT amount) unique_txn_types,
    COUNT(*) cnt
FROM transactions
WHERE is_flagged = TRUE AND is_flagged = TRUE
GROUP BY txn_day, txn_type
ORDER BY txn_type DESC, cnt DESC;
