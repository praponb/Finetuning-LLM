WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        attr_value,
        ROUND(status_code / latency_ms, 2) status_code_per_latency_ms,
        LENGTH(k) k_length
    FROM exploded
    WHERE k IS NOT NULL
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, k_length * status_code_per_latency_ms k_length_times_status_code_per_latency_ms
    FROM base
    WHERE k_length = 58
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_per_latency_ms, id, k) AS latest_rank
        FROM base_app_logs
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    latency_ms,
    status_code,
    k,
    status_code_per_latency_ms,
    k_length,
    k_length_times_status_code_per_latency_ms,
    latest_rank
FROM latest
ORDER BY status_code_per_latency_ms DESC NULLS FIRST;
