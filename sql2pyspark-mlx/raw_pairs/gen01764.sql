WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM exploded
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        SUM(is_new_session) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        host,
        log_ts,
        key
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, key ORDER BY log_ts DESC, id, key) = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM latest
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        key,
        SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        log_ts,
        key,
        burst_id
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key, host ORDER BY burst_id, id, key) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT_IF(key = 'trace') AS trace_events,
    APPROX_PERCENTILE(burst_id, 0.5) AS burst_id_p50,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN burst_id >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(key = 'trace') DESC) AS trace_events_dense_rank
FROM deduped
GROUP BY CASE WHEN burst_id >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', log_ts)
