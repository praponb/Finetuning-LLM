WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM exploded
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) AS prev_log_ts_2
    FROM islands
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts_2 IS NULL OR log_ts > prev_log_ts_2 + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Ranking and gap features
SELECT
    *,
    MAX(session_id) OVER (PARTITION BY level) AS level_max_session_id,
    SUM(new_group_flag) OVER (PARTITION BY level ORDER BY prev_log_ts_2, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_new_group_flag,
    AVG(new_group_flag) OVER (PARTITION BY key ORDER BY CAST(prev_log_ts_2 AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS new_group_flag_avg_7d,
    ROW_NUMBER() OVER (ORDER BY prev_log_ts DESC, id, key) AS seq_no,
    NTILE(4) OVER (PARTITION BY key ORDER BY is_new_burst DESC, id, key) AS is_new_burst_bucket
FROM sessionized
