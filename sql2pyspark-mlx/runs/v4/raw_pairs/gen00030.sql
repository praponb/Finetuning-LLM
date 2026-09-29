WITH flattened AS (
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
scored AS (
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v,
        SUM(status_code) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) status_code_sum_30m,
        MIN(status_code) OVER (PARTITION BY key) key_min_status_code,
        LAST_VALUE(key) IGNORE NULLS OVER w_running last_known_key,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts DESC, id, key) event_seq,
        LAG(status_code, 1, 0) OVER (PARTITION BY key ORDER BY log_ts, id, key) prev_status_code
    FROM flattened
    WINDOW w_running AS (ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
prev_events AS (
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM scored
),
with_gaps AS (
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
report AS (
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v,
        key_min_status_code,
        last_known_key,
        prev_status_code,
        prev_log_ts,
        CASE WHEN key RLIKE '^(region|version)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS key_zone
    FROM islands
    ORDER BY id NULLS FIRST
)
SELECT
    id,
    log_ts,
    last_known_key,
    prev_status_code,
    prev_log_ts
FROM report;
