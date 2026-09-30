WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        key,
        v,
        UPPER(service) AS service_upper
    FROM exploded
    WHERE key IS DISTINCT FROM 'trace'
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(key, 'Unknown') AS key_filled
    FROM filtered
    WHERE key IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', key, service) AS key_service_key,
        LENGTH(service) AS service_length,
        COALESCE(key, 'OTHER') AS key_filled_2
    FROM filtered_app_logs
    WHERE key IS NOT NULL
      AND key IS NOT DISTINCT FROM 'trace'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        level,
        key,
        v,
        service_upper,
        key_filled,
        key_service_key,
        service_length,
        key_filled_2
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY service_length DESC, id, key) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    key,
    SIZE(COLLECT_SET(level)) AS num_levels,
    AVG(service_length) FILTER (WHERE level = 'DEBUG') AS debug_avg_service_length,
    SUM(CASE WHEN level = 'INFO' THEN service_length ELSE 0 END) AS info_service_length
FROM deduped
GROUP BY key
ORDER BY num_levels DESC NULLS LAST;
