SELECT
    is_flagged,
    MONTH(txn_time) txn_month,
    COUNT(*) row_count,
    AVG(amount) avg_amount
FROM transactions
WHERE amount > 3000
GROUP BY is_flagged, MONTH(txn_time);
