WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER w_recent AS latency_ms_rank
    FROM exploded
    WHERE status_code <= 434
    WINDOW w_recent AS (PARTITION BY level, host ORDER BY latency_ms NULLS LAST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', host, level) AS host_level_key,
        FLOOR(status_code) AS status_code_whole,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM with_windows
    WHERE attr_key IS NOT NULL
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_code + status_code_whole AS status_code_plus_status_code_whole,
        FLOOR(latency_ms) AS latency_ms_whole
    FROM valid
    WHERE attr_key IS DISTINCT FROM 'version'
      AND attr_key IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code_plus_status_code_whole NULLS FIRST) AS status_code_plus_status_code_whole_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY service, host) AS corr_latency_ms_status_code,
        MAX(status_code_whole) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_whole_max_30m,
        CUME_DIST() OVER (ORDER BY status_code_whole DESC) AS status_code_whole_cume_dist
    FROM valid_app_logs
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    log_ts,
    level,
    status_code,
    attr_value,
    latency_ms_rank,
    host_level_key,
    status_code_plus_status_code_whole,
    latency_ms_whole,
    status_code_plus_status_code_whole_cume_dist,
    status_code_whole_max_30m,
    status_code_whole_cume_dist,
    AVG(status_code_whole_max_30m) OVER w_running AS status_code_whole_max_30m_avg_15m,
    MAX(latency_ms_whole) OVER (PARTITION BY MONTH(log_ts), host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_whole_max_2h
FROM scored
WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW);
