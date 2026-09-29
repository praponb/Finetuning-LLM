WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
ranked AS (
    SELECT *
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY status_code, id, attr_key) = 1
),
base AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        value,
        CONCAT(service, ' - ', attr_key) AS service_attr_key,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM ranked
    WHERE log_ts > TIMESTAMP '2025-06-01 07:00:00'
      AND attr_key IS NOT NULL
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts DESC, id, attr_key) = 1
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            attr_key,
            level,
            service,
            status_code
        FROM deduped
    ) src
    PIVOT (
        MAX(status_code)
        FOR service IN ('auth', 'checkout')
    )
)
SELECT
    attr_key,
    level,
    checkout,
    CASE WHEN level RLIKE '^(ERROR|DEBUG)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS level_zone
FROM pivoted
ORDER BY attr_key
