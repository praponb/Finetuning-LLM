WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
parsed AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM flattened
    WHERE k = 'trace'
),
prev_events AS (
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_log_ts
    FROM parsed
),
with_gaps AS (
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        status_code_high,
        is_flagged,
        prev_log_ts,
        is_new_session,
        island_id,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS event_seq,
        NTILE(3) OVER (PARTITION BY host ORDER BY is_flagged, id, k) AS is_flagged_bucket
    FROM sessionized
),
grouped AS (
    SELECT
        host,
        SORT_ARRAY(COLLECT_SET(k)) AS k_list,
        MIN(log_ts) AS started_at,
        COUNT_IF(k = 'version') AS version_events,
        SUM(CASE WHEN host = 'h3' THEN status_code_high ELSE 0 END) AS h3_status_code_high,
        AVG(latency_ms) AS avg_latency_ms,
        SIZE(COLLECT_SET(host)) AS num_hosts
    FROM with_windows
    GROUP BY host
)
SELECT
    host,
    k_list,
    version_events,
    h3_status_code_high,
    avg_latency_ms,
    num_hosts
FROM grouped
ORDER BY host
LIMIT 20
