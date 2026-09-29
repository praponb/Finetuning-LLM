WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        attr_value,
        SUBSTRING(k, 1, 4) k_prefix
    FROM lines
    WHERE k IS NOT NULL
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(level) level_upper,
        COALESCE(k, 'N/A') k_filled
    FROM cleaned
    WHERE service = 'auth'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        SUM(latency_ms) OVER (PARTITION BY service) service_sum_latency_ms
    FROM cleaned_app_logs
),
metrics AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY latency_ms DESC) latency_ms_dense_rank
    FROM enriched
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY latency_ms_dense_rank DESC NULLS LAST) latency_ms_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY latency_ms) latency_ms_dense_rank_2
    FROM metrics
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        latency_ms,
        service_sum_latency_ms,
        latency_ms_dense_rank_rank,
        latency_ms_dense_rank_2,
        latency_ms + latency_ms_dense_rank latency_ms_plus_latency_ms_dense_rank,
        CASE WHEN latency_ms >= 2382 THEN 'High' WHEN latency_ms >= 937 THEN 'Medium' ELSE 'Low' END latency_ms_level
    FROM scored
    WHERE service_sum_latency_ms >= 6176
)
SELECT
    id,
    latency_ms_dense_rank_rank,
    latency_ms_dense_rank_2,
    latency_ms_plus_latency_ms_dense_rank,
    latency_ms_level
FROM filtered
ORDER BY latency_ms_dense_rank_rank DESC NULLS LAST;
