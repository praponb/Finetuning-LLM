WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms DESC, id, k) AS rn
    FROM exploded
    QUALIFY rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT host
    FROM deduped
    INTERSECT
    SELECT host
    FROM app_logs
    WHERE service IN ('search', 'auth', 'checkout')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.host = k.host
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        value,
        rn,
        TRIM(service) AS clean_service,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM reconciled
    WHERE k IS NOT NULL
      AND rn = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        service,
        level,
        latency_ms,
        value,
        rn,
        k_filled,
        LENGTH(level) AS level_length
    FROM base
    WHERE service <> 'search'
)
-- Grouped metrics
SELECT
    CASE WHEN level_length >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_length_tier,
    CASE WHEN latency_ms >= 2221 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    COUNT_IF(level = 'INFO') AS info_events,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN level_length >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT_IF(level = 'INFO') DESC) AS info_events_percent_rank
FROM valid
GROUP BY level_length_tier, latency_ms_tier
ORDER BY latency_ms_tier NULLS LAST;
