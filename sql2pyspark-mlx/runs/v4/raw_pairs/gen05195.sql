WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        TRIM(key) AS clean_key
    FROM flattened
    WHERE log_ts > TIMESTAMP '2025-06-01 18:00:00'
      AND service <> 'checkout'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM base
    WHERE service = 'auth'
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(service)) AS service_title_2
    FROM cleaned
    WHERE service IN ('search', 'billing')
)
-- Aggregate with conditional metrics
SELECT
    service,
    COUNT_IF(service = 'search') AS search_events,
    SIZE(COLLECT_SET(service)) AS num_services,
    SIZE(COLLECT_SET(service)) AS num_services_2,
    SIZE(COLLECT_SET(service)) AS num_services_3,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows
FROM base_app_logs
GROUP BY service
