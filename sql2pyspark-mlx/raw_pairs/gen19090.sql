WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
base AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        UPPER(host) AS host_upper,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM flattened
    WHERE log_ts >= TIMESTAMP '2025-06-01 12:00:00'
),
matched_keys AS (
    SELECT id, host
    FROM base
    INTERSECT ALL
    SELECT id, host
    FROM app_logs
    WHERE level = 'DEBUG'
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id AND s.host = k.host
),
valid AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        resolved_key,
        INITCAP(TRIM(key)) AS key_title,
        LOWER(host) AS host_lower,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM verified
    WHERE log_ts > TIMESTAMP '2025-06-01 12:00:00'
      AND key IS NOT NULL
)
SELECT
    id,
    service,
    host,
    log_ts,
    resolved_key,
    key_title,
    host_lower,
    key_filled
FROM valid
ORDER BY host DESC NULLS LAST
