WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(key, ', ', level) AS key_level
    FROM flattened
    WHERE key IS NOT NULL
      AND log_ts > TIMESTAMP '2025-06-01 06:00:00'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', log_ts) AS log_ts_period,
        COALESCE(key, 'NONE') AS key_clean
    FROM valid
    WHERE key IS DISTINCT FROM 'region'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN level = 'DEBUG' THEN 1 ELSE 0 END AS is_debug,
        COALESCE(key, 'NONE') AS key_filled
    FROM filtered
    WHERE key IS NOT NULL
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(host) AS clean_host,
        COALESCE(key, 'UNKNOWN') AS key_clean_2
    FROM cleaned
    WHERE key IS NOT NULL
      AND log_ts >= TIMESTAMP '2025-06-01 08:00:00'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN is_debug >= 79 THEN 'LARGE' ELSE 'SMALL' END AS is_debug_tier,
        MIN(log_ts) AS started_at,
        COUNT(CASE WHEN key = 'version' THEN 1 END) AS version_count
    FROM filtered_app_logs
    GROUP BY CASE WHEN is_debug >= 79 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    is_debug_tier,
    started_at,
    version_count
FROM totals;
