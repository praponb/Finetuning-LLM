WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
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
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level = 'WARN' THEN 1 ELSE 0 END AS is_warn
    FROM flattened
    WHERE k IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) AS prev_log_ts
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value,
        is_warn,
        SUM(is_new_burst) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT service, id
    FROM sessions_app_logs
    INTERSECT ALL
    SELECT service, id
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 11 AND 15
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_app_logs s
    INNER JOIN kept_keys k ON s.service = k.service AND s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_avg_1h,
        CORR(status_code, latency_ms) OVER (PARTITION BY k) AS corr_status_code_latency_ms,
        AVG(status_code) OVER (PARTITION BY service, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m,
        SUM(is_warn) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_warn_sum_2h
    FROM verified
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        k,
        SUM(corr_status_code_latency_ms) AS total_corr_status_code_latency_ms,
        COUNT(CASE WHEN service = 'auth' THEN 1 END) AS auth_count,
        APPROX_PERCENTILE(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles
    FROM metrics
    GROUP BY k
    HAVING SUM(corr_status_code_latency_ms) < 1000000
)
SELECT
    k,
    auth_count,
    session_id_quartiles
FROM totals
ORDER BY auth_count
