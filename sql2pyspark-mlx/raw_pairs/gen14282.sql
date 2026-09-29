WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        status_code_high,
        AVG(status_code_high) OVER (PARTITION BY service) AS service_avg_status_code_high
    FROM flattened_attrs
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(host, ', ', service) AS host_service
    FROM metrics
    WHERE service NOT IN ('search', 'auth', 'checkout')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(service_avg_status_code_high * 1.1, 2) AS service_avg_status_code_high_adjusted
    FROM base
    WHERE service NOT IN ('billing', 'search', 'auth')
)
SELECT
    id,
    host,
    status_code_high,
    service_avg_status_code_high,
    host_service,
    service_avg_status_code_high_adjusted,
    CASE WHEN host RLIKE '^(h2|h4)$' THEN 'PRIORITY' WHEN host RLIKE '^(h1|h3)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS host_zone
FROM filtered
ORDER BY service_avg_status_code_high ASC NULLS LAST
