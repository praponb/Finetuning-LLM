WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(level) AS clean_level
    FROM flattened
    WHERE level = 'INFO'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(service) AS clean_service
    FROM cleaned
    WHERE k IS DISTINCT FROM 'version'
      AND k IS NOT NULL
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN level IN ('INFO', 'WARN') THEN 'Group A' ELSE 'Other' END AS level_group,
        CONCAT(service, '/', level) AS service_level
    FROM filtered
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'region'
),
filtered_app_logs_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', k, service) AS k_service_key
    FROM filtered_app_logs
    WHERE k IS NOT DISTINCT FROM 'trace'
)
SELECT
    service,
    attr_value,
    clean_level,
    level_group,
    service_level,
    k_service_key,
    CASE WHEN service RLIKE '^(checkout|auth)$' THEN 'PRIORITY' WHEN service RLIKE '^(billing|search)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS service_zone
FROM filtered_app_logs_2
ORDER BY service DESC NULLS LAST
