WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    COUNT_IF(txn_type = 'REFUND') AS refund_events,
    COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows
FROM islands
GROUP BY CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', txn_time)
HAVING num_txn_types > 1
