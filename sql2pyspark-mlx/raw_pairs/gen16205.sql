WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        key,
        v,
        CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END AS is_error,
        COALESCE(key, 'OTHER') AS key_filled
    FROM exploded
    WHERE level != 'DEBUG'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(level)) AS level_title,
        COALESCE(key, 'UNKNOWN') AS key_filled_2
    FROM filtered
    WHERE is_error BETWEEN 63 AND 64
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN is_error >= 71 THEN 'High' WHEN is_error >= 37 THEN 'Medium' ELSE 'Low' END AS is_error_band,
        TRIM(level) AS clean_level,
        COALESCE(key, 'OTHER') AS key_clean
    FROM valid
    WHERE key NOT IN ('version', 'region')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_error) OVER (PARTITION BY level) AS level_avg_is_error,
        MIN(is_error) OVER (PARTITION BY level, key) AS level_min_is_error,
        PERCENT_RANK() OVER (PARTITION BY level, key ORDER BY is_error) AS is_error_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY is_error) AS is_error_percent_rank_2
    FROM base
    WHERE level = 'WARN'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        key,
        level_avg_is_error
    FROM enriched
) src
PIVOT (
    SUM(level_avg_is_error) AS value_sum,
    COUNT(1) AS cnt
    FOR key IN ('region' AS region, 'version' AS version)
);
