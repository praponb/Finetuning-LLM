WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        latency_ms,
        k,
        has_fault,
        status_code_high,
        CASE WHEN k IN ('region', 'version') THEN 'Core' ELSE 'Other' END AS k_group,
        COALESCE(k, 'UNKNOWN') AS resolved_k
    FROM flattened_attrs
    WHERE k IS NOT NULL
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            service,
            k,
            latency_ms
        FROM valid
    ) src
    PIVOT (
        SUM(latency_ms) AS value_sum,
        COUNT(1) AS item_count
        FOR k IN ('trace', 'region', 'version')
    )
)
SELECT
    service,
    trace_value_sum,
    region_item_count,
    version_value_sum,
    version_item_count,
    CASE WHEN service RLIKE '^(checkout|billing)$' THEN 'GROUP_B' WHEN service RLIKE '^(auth|search)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS service_zone
FROM pivoted
ORDER BY service DESC NULLS FIRST
