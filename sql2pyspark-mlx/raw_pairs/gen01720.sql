WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
kept_keys AS (
    -- Set difference on keys
    SELECT service
    FROM exploded
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE level <> 'DEBUG'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN kept_keys k ON s.service = k.service
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms NULLS LAST) AS latency_ms_rank,
        PERCENT_RANK() OVER (PARTITION BY key, service ORDER BY latency_ms) AS latency_ms_percent_rank,
        COUNT(latency_ms) OVER (PARTITION BY level) AS level_count_latency_ms
    FROM verified
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(level) AS clean_level,
        COALESCE(key, 'NONE') AS key_filled
    FROM with_windows
    WHERE key IS DISTINCT FROM 'trace'
      AND key IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        value,
        latency_ms_rank,
        latency_ms_percent_rank,
        level_count_latency_ms,
        clean_level,
        key_filled,
        LOWER(key) AS key_lower
    FROM base
    WHERE latency_ms_percent_rank > 0.71
)
SELECT
    id,
    level,
    latency_ms_rank,
    latency_ms_percent_rank,
    level_count_latency_ms,
    clean_level,
    key_filled,
    key_lower,
    CASE WHEN level RLIKE '^(DEBUG|INFO)$' THEN 'GROUP_B' WHEN level RLIKE '^(WARN|ERROR)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS level_zone
FROM filtered
ORDER BY level NULLS LAST, level_count_latency_ms DESC
