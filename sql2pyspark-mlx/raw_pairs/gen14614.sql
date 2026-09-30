WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    APPROX_PERCENTILE(session_start_flag, 0.5) AS session_start_flag_p50,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    MAX(txn_time) AS ended_at
FROM islands
GROUP BY DATE_TRUNC('YEAR', txn_time)
HAVING COUNT(DISTINCT txn_type) > 1
ORDER BY distinct_txn_types DESC NULLS FIRST
