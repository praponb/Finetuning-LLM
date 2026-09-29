WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM flattened
    WHERE attr_key IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        attr_key,
        latency_ms_filled,
        attr_key_clean,
        CASE WHEN latency_ms >= 2260 THEN 'High' WHEN latency_ms >= 833 THEN 'Medium' ELSE 'Low' END AS latency_ms_tier,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM filtered
    WHERE latency_ms < 1509
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key_2
    FROM base
    WHERE level = 'ERROR'
)
-- Window metrics over several different partitions
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY latency_ms_filled DESC NULLS FIRST) AS latency_ms_filled_dense_rank,
    CUME_DIST() OVER (PARTITION BY attr_key ORDER BY latency_ms_filled DESC NULLS LAST) AS latency_ms_filled_cume_dist
FROM cleaned
