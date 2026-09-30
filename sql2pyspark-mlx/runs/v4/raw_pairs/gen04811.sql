WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM exploded
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        status_code_high,
        has_fault,
        UPPER(service) AS service_upper
    FROM flattened_attrs
    WHERE status_code_high >= 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(has_fault) AS has_fault_whole
    FROM base
    WHERE status_code_high <= 2
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_code_high / NULLIF(has_fault, 0) AS status_code_high_per_has_fault,
        status_code_high - has_fault AS status_code_high_minus_has_fault
    FROM valid
    WHERE status_code_high <= 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code_high_per_has_fault DESC) AS status_code_high_per_has_fault_percent_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code_high DESC) AS status_code_high_dense_rank,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code_high_per_has_fault) AS status_code_high_per_has_fault_cume_dist
    FROM base_app_logs
)
SELECT
    service,
    has_fault,
    service_upper,
    has_fault_whole,
    status_code_high_per_has_fault,
    status_code_high_minus_has_fault,
    status_code_high_per_has_fault_percent_rank,
    status_code_high_dense_rank
FROM metrics
ORDER BY status_code_high_minus_has_fault DESC NULLS FIRST
