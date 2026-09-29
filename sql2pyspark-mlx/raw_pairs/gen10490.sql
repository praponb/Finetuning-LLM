WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE service IN ('checkout', 'billing')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        STDDEV_SAMP(is_flagged) OVER (PARTITION BY service) AS service_stddev_samp_is_flagged,
        DENSE_RANK() OVER (ORDER BY is_flagged) AS is_flagged_dense_rank,
        RANK() OVER (PARTITION BY service ORDER BY is_flagged) AS is_flagged_rank
    FROM parsed
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(is_flagged_dense_rank / is_flagged_rank, 2) AS is_flagged_dense_rank_per_is_flagged_rank
    FROM with_windows
    WHERE is_flagged_dense_rank < 8
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        is_flagged_dense_rank,
        is_flagged_rank,
        is_flagged_dense_rank_per_is_flagged_rank,
        CEIL(is_flagged_dense_rank) AS is_flagged_dense_rank_whole
    FROM valid
    WHERE is_flagged_rank > 7
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(is_flagged_dense_rank, 2) AS is_flagged_dense_rank_rounded
    FROM filtered
    WHERE is_flagged_dense_rank_whole > 75
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        is_flagged_rank,
        is_flagged_dense_rank_per_is_flagged_rank,
        is_flagged_dense_rank_whole,
        is_flagged_dense_rank_rounded,
        CAST(is_flagged_dense_rank_per_is_flagged_rank AS INT) AS is_flagged_dense_rank_per_is_flagged_rank_int,
        CAST(is_flagged_dense_rank AS STRING) AS is_flagged_dense_rank_str
    FROM base
    WHERE is_flagged_rank <> 7
)
-- Grouped metrics
SELECT
    CASE WHEN is_flagged_dense_rank_per_is_flagged_rank >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_dense_rank_per_is_flagged_rank_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count
FROM base_app_logs
GROUP BY 1
HAVING COUNT(*) < 1000
ORDER BY event_count DESC NULLS FIRST;
