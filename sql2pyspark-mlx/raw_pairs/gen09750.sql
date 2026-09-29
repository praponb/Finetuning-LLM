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
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        host,
        level,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(level, 1, 4) AS level_prefix
    FROM parsed
    WHERE host != 'h4'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        level,
        status_code,
        status_code_high,
        is_flagged,
        level_prefix,
        TRIM(host) AS clean_host,
        LENGTH(host) AS host_length
    FROM filtered
    WHERE status_code > 282
),
summary AS (
    -- Grouped metrics
    SELECT
        host,
        level,
        COUNT(DISTINCT level) AS distinct_levels,
        COUNT(DISTINCT level) AS distinct_levels_2
    FROM base
    GROUP BY CUBE (host, level)
)
SELECT host, distinct_levels
FROM summary
