WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms,
        NTILE(4) OVER (PARTITION BY host, k ORDER BY latency_ms DESC, id, k) AS latency_ms_bucket,
        SUM(latency_ms) OVER (PARTITION BY host, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_sum_2h,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_max_1h
    FROM exploded
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(new_group_flag) OVER (PARTITION BY service, host ORDER BY latency_ms_max_1h, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_new_group_flag,
        ROUND(latency_ms_max_1h / SUM(latency_ms_max_1h) OVER (PARTITION BY host, service), 4) AS latency_ms_max_1h_share
    FROM sessions_app_logs
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics_app_logs
),
sessions_app_logs_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessions_app_logs_2
),
sessions_app_logs_3 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_log_ts
    FROM sessions_app_logs_3
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms_max_1h, id, k) AS event_seq,
    LAG(session_id, 2) OVER (PARTITION BY k ORDER BY session_no, id, k) AS prev_session_id,
    AVG(latency_ms_max_1h) OVER (ORDER BY session_no, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS latency_ms_max_1h_avg_6_prior
FROM islands
