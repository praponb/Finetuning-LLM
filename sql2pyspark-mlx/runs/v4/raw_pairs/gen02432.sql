WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
kept_keys AS (
    -- Set intersection on keys
    SELECT service, id
    FROM flattened
    INTERSECT
    SELECT service, id
    FROM app_logs
    WHERE level = 'WARN'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.service = k.service AND s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        log_ts,
        attr_key,
        attr_value,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM reconciled
    WHERE attr_key IS NOT NULL
      AND log_ts >= '2023-08-01'
)
SELECT
    service,
    log_ts,
    attr_key,
    attr_value,
    attr_key_filled,
    CASE WHEN service RLIKE '^(search|auth)$' THEN 'CORE' ELSE 'EXTENDED' END AS service_zone
FROM valid
ORDER BY service_zone DESC NULLS LAST
