WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
with_windows AS (
    SELECT
        *,
        MAX(status_code) OVER (PARTITION BY attr_key, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_max_15m,
        SUM(status_code) OVER (PARTITION BY DATE_TRUNC('YEAR', log_ts), host ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        AVG(latency_ms) OVER (PARTITION BY attr_key, host ORDER BY latency_ms, id, attr_key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS latency_ms_avg_5,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms NULLS FIRST) AS latency_ms_cume_dist
    FROM lines
),
enriched AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY attr_key, host ORDER BY status_code DESC, id, attr_key) AS status_code_bucket,
        NTILE(3) OVER (PARTITION BY YEAR(log_ts) ORDER BY cumulative_status_code, id, attr_key) AS cumulative_status_code_bucket
    FROM with_windows
),
valid AS (
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        attr_value,
        status_code_max_15m,
        cumulative_status_code,
        latency_ms_cume_dist,
        status_code_bucket,
        cumulative_status_code_bucket,
        CAST(latency_ms_avg_5 AS STRING) AS latency_ms_avg_5_str,
        ROUND(status_code_max_15m / status_code_bucket, 2) AS status_code_max_15m_per_status_code_bucket
    FROM enriched
    WHERE attr_key IS DISTINCT FROM 'version'
      AND latency_ms_avg_5 > 736
),
filtered AS (
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        status_code_max_15m,
        latency_ms_cume_dist,
        cumulative_status_code_bucket,
        latency_ms_avg_5_str,
        status_code_max_15m_per_status_code_bucket,
        LOWER(attr_key) AS attr_key_lower,
        COALESCE(attr_key, 'NONE') AS resolved_attr_key
    FROM valid
    WHERE attr_key IS NOT NULL
      AND cumulative_status_code_bucket < 8
),
prev_events AS (
    SELECT *, LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM filtered
),
with_gaps AS (
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_app_logs AS (
    SELECT
        id,
        log_ts,
        attr_key,
        status_code_max_15m_per_status_code_bucket,
        resolved_attr_key,
        SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    session_id,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN attr_key = 'version' THEN 1 END), 0), 2) AS session_id_per_version,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count
FROM sessions_app_logs
GROUP BY session_id
ORDER BY session_id NULLS FIRST;
