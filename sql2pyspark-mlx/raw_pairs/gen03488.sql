WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        LEAD(status_code, 1, 0) OVER w_part AS next_status_code,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) AS corr_latency_ms_status_code,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior
    FROM flattened
    WINDOW w_part AS (ORDER BY latency_ms ASC, id ASC, attr_key ASC)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE status_code >= 360
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(corr_latency_ms_status_code, 0) AS corr_latency_ms_status_code_filled,
        CASE WHEN attr_key = 'region' THEN 1 ELSE 0 END AS is_region,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM reconciled
    WHERE attr_key IS NOT DISTINCT FROM 'region'
      AND status_code_avg_2_prior <= 276
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    service,
    kpi,
    metric_value
FROM (
    SELECT
        service,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code_filled
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (corr_latency_ms_status_code, corr_latency_ms_status_code_filled)
) u
