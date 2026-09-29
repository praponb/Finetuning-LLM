WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
enriched AS (
    SELECT *, MAX(status_code) OVER (PARTITION BY key) key_max_status_code
    FROM flattened
    WHERE status_code >= 291
),
base AS (
    SELECT
        id,
        level,
        attr_value,
        key_max_status_code,
        TRIM(level) clean_level
    FROM enriched
    WHERE log_ts > TIMESTAMP '2025-06-01 14:00:00'
      AND service = 'billing'
),
base_app_logs AS (
    SELECT
        id,
        level,
        clean_level,
        UPPER(level) level_upper
    FROM base
    WHERE level = 'WARN'
),
filtered AS (
    SELECT *, LOWER(level) level_lower
    FROM base_app_logs
    WHERE level IN ('ERROR', 'INFO', 'WARN')
)
SELECT
    id,
    level,
    clean_level,
    CASE WHEN level RLIKE '^(INFO|ERROR)$' THEN 'CORE' ELSE 'GROUP_A' END AS level_zone
FROM filtered
ORDER BY level ASC NULLS FIRST
