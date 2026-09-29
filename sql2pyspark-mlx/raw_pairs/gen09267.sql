SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    ROUND(AVG(amount), 2) AS avg_amount,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    SUM(amount) AS total_amount,
    MAX(txn_time) AS last_seen
FROM transactions
GROUP BY DATE_TRUNC('QUARTER', txn_time)
ORDER BY avg_amount DESC NULLS FIRST
