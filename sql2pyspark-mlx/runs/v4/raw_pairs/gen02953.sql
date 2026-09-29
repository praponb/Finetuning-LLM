SELECT
    merchant,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    txn_type,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    COUNT(*) AS cnt,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS amount_per_refund
FROM transactions
GROUP BY merchant, week_start, txn_type
ORDER BY avg_amount DESC;
