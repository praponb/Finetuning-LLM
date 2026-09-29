WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    CASE WHEN amount >= 3600 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    PERCENTILE_APPROX(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    GROUPING(DATE_TRUNC('QUARTER', txn_time)) AS grouping_id
FROM sessions_transactions
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', txn_time), CASE WHEN amount >= 3600 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', txn_time)), ())
ORDER BY num_merchants DESC NULLS LAST;
