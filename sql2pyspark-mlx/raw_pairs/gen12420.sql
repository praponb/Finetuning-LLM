WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM flattened
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        SUM(session_start_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        session_no,
        MAX(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_status_code,
        SUM(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_status_code,
        AVG(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_status_code,
        COUNT(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_status_code
    FROM sessionized
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) AS prev_log_ts
    FROM metrics
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        session_no,
        running_max_status_code,
        running_avg_status_code,
        running_count_status_code,
        SUM(is_new_session) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        k,
        session_no,
        running_max_status_code,
        running_avg_status_code,
        ROW_NUMBER() OVER (PARTITION BY k, host ORDER BY status_code DESC, id, k) seq_no,
        LAG(running_max_status_code, 1) OVER (PARTITION BY host, k ORDER BY running_count_status_code, id, k) prev_running_max_status_code,
        AVG(running_count_status_code) OVER (PARTITION BY k) k_avg_running_count_status_code,
        SUM(status_code) OVER (PARTITION BY MONTH(log_ts)) log_ts_month_sum_status_code,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts, id, k) seq_no_2
    FROM sessions_app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN k_avg_running_count_status_code >= 16 THEN 'LARGE' ELSE 'SMALL' END AS k_avg_running_count_status_code_tier,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(k = 'version') AS version_events
FROM with_windows
GROUP BY k_avg_running_count_status_code_tier
HAVING avg_status_code < 1000000
ORDER BY k_avg_running_count_status_code_tier DESC;
