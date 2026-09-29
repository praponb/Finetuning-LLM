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
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        status_code,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE status_code > 303
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status_code,
        value,
        status_code_high,
        is_flagged,
        FLOOR(status_code_high) status_code_high_whole,
        status_code_high * is_flagged status_code_high_times_is_flagged
    FROM parsed
    WHERE is_flagged > 0
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, status_code_high + is_flagged status_code_high_plus_is_flagged
    FROM base
    WHERE status_code_high > 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status_code,
        status_code_high,
        is_flagged,
        status_code_high_plus_is_flagged,
        CASE WHEN status_code >= 421 THEN 'High' WHEN status_code >= 258 THEN 'Medium' ELSE 'Low' END status_code_tier,
        status_code_high_whole / NULLIF(status_code_high, 0) status_code_high_whole_per_status_code_high
    FROM cleaned
    WHERE status_code_high = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_high_plus_is_flagged) status_code_high_plus_is_flagged_dense_rank,
        CUME_DIST() OVER (ORDER BY status_code_high_whole_per_status_code_high DESC NULLS FIRST) status_code_high_whole_per_status_code_high_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code_high DESC NULLS FIRST) status_code_high_cume_dist
    FROM filtered
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_high_plus_is_flagged_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_plus_is_flagged_dense_rank_tier,
    CASE WHEN is_flagged >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_tier,
    AVG(status_code_high_plus_is_flagged) AS avg_status_code_high_plus_is_flagged,
    MAX(status_code_high) AS max_status_code_high,
    ROUND(AVG(is_flagged), 2) AS avg_is_flagged,
    APPROX_PERCENTILE(status_code_high_plus_is_flagged_dense_rank, 0.5) AS status_code_high_plus_is_flagged_dense_rank_p50,
    APPROX_PERCENTILE(is_flagged, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_quartiles,
    APPROX_PERCENTILE(is_flagged, 0.9) AS is_flagged_p90
FROM enriched
GROUP BY CASE WHEN status_code_high_plus_is_flagged_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_flagged >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(is_flagged), 2) > 0
