WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        attr_key,
        v,
        LENGTH(level) AS level_length
    FROM flattened
    WHERE attr_key NOT IN ('trace', 'region', 'version')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY attr_key ORDER BY level_length DESC, id, attr_key) AS level_length_bucket,
        SUM(level_length) OVER (PARTITION BY attr_key, level ORDER BY level_length, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_level_length
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(level_length_bucket) OVER (PARTITION BY level ORDER BY level_length, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_level_length_bucket,
        NTILE(3) OVER (PARTITION BY level, attr_key ORDER BY level_length_bucket DESC, id, attr_key) AS level_length_bucket_bucket
    FROM enriched
)
SELECT
    id,
    level,
    attr_key,
    v,
    level_length,
    level_length_bucket,
    cumulative_level_length,
    level_length_bucket_bucket
FROM scored
ORDER BY level NULLS FIRST;
