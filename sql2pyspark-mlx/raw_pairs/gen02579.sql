WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(latency_ms, 1) AS latency_ms_rounded,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM flattened
    WHERE key IS NOT NULL
      AND service = 'search'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        latency_ms_rounded,
        resolved_key,
        CASE WHEN latency_ms >= 2284 THEN 'High' WHEN latency_ms >= 431 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        FLOOR(latency_ms_rounded) AS latency_ms_rounded_whole
    FROM base
    WHERE key IS NOT NULL
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        latency_ms_rounded,
        latency_ms_level,
        latency_ms_rounded_whole - latency_ms_rounded AS latency_ms_rounded_whole_minus_latency_ms_rounded,
        COALESCE(latency_ms_rounded, 0) AS latency_ms_rounded_filled
    FROM filtered
    WHERE latency_ms_rounded > 69
),
scored AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (ORDER BY latency_ms_rounded DESC NULLS LAST) AS latency_ms_rounded_rank
    FROM base_app_logs
    WHERE latency_ms_rounded BETWEEN 34 AND 47
),
report AS (
    SELECT
        id,
        latency_ms_rounded,
        latency_ms_level,
        latency_ms_rounded_whole_minus_latency_ms_rounded,
        latency_ms_rounded_filled
    FROM scored
    ORDER BY id DESC
)
SELECT
    latency_ms_rounded,
    latency_ms_level,
    latency_ms_rounded_filled
FROM report;
