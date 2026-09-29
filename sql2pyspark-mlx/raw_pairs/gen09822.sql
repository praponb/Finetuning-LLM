WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
unpacked AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4
    FROM lines
    WHERE level = 'INFO'
),
valid AS (
    SELECT *, ROUND(flag_bit4 / latency_ms, 2) AS flag_bit4_per_latency_ms
    FROM unpacked
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'region'
),
valid_app_logs AS (
    SELECT
        *,
        latency_ms / NULLIF(status_code_high, 0) AS latency_ms_per_status_code_high,
        ROUND(latency_ms * 1.1, 2) AS latency_ms_adjusted
    FROM valid
    WHERE key NOT IN ('version', 'region')
),
valid_app_logs_2 AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        v,
        status_code_high,
        flag_bit4,
        flag_bit4_per_latency_ms,
        latency_ms_per_status_code_high,
        latency_ms_adjusted,
        ROUND(latency_ms * 1.1, 2) AS latency_ms_scaled
    FROM valid_app_logs
    WHERE key IS NOT NULL
)
SELECT
    id,
    level,
    latency_ms,
    v,
    status_code_high,
    flag_bit4,
    flag_bit4_per_latency_ms,
    latency_ms_per_status_code_high,
    latency_ms_scaled,
    CASE WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'PRIORITY' WHEN level RLIKE '^(WARN|ERROR)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS level_zone
FROM valid_app_logs_2
ORDER BY id NULLS FIRST, status_code_high DESC
