WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_value,
        TRIM(service) clean_service,
        LOWER(level) level_lower,
        COALESCE(k, 'NONE') k_clean
    FROM flattened
    WHERE log_ts >= TO_DATE('2025-06-01')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        level,
        clean_service,
        level_lower,
        LOWER(host) host_lower
    FROM base
    WHERE log_ts > TO_DATE('2025-06-01')
      AND service IN ('billing', 'checkout')
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(host, ' - ', level) host_level
    FROM valid
    WHERE level IN ('WARN', 'ERROR', 'DEBUG')
)
SELECT
    id,
    host,
    level,
    clean_service,
    host_lower,
    host_level,
    CASE WHEN host RLIKE '^(h4|h2)$' THEN 'CORE' ELSE 'EXTENDED' END AS host_zone
FROM valid_app_logs
ORDER BY id;
