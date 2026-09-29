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
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        latency_ms,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        latency_ms,
        flag_bit4,
        status_code_high,
        flag_bit4 / NULLIF(status_code_high, 0) AS flag_bit4_per_status_code_high,
        CONCAT_WS('/', service, level) AS service_level_key
    FROM flattened_attrs
    WHERE level IN ('INFO', 'ERROR', 'DEBUG')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        level,
        latency_ms,
        flag_bit4,
        status_code_high,
        flag_bit4_per_status_code_high,
        service_level_key,
        TRIM(level) AS clean_level
    FROM valid
    WHERE latency_ms < 2136
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(flag_bit4, 1) AS flag_bit4_rounded
    FROM cleaned
    WHERE status_code_high BETWEEN 1 AND 2
)
SELECT
    id,
    level,
    latency_ms,
    flag_bit4,
    status_code_high,
    clean_level,
    flag_bit4_rounded
FROM base
ORDER BY level
