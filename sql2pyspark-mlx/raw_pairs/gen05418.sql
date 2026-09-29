WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(key, 1, 2) AS key_prefix,
        COALESCE(key, 'OTHER') AS key_clean
    FROM lines
    WHERE key IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(service) AS service_lower,
        LOWER(level) AS level_lower
    FROM base
    WHERE key IS NOT NULL
      AND key = 'version'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        key,
        service_lower,
        TRIM(service) AS clean_service,
        TRIM(level) AS clean_level
    FROM cleaned
    WHERE key IS NOT NULL
      AND key NOT IN ('trace', 'region')
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(service) AS service_upper,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM filtered
    WHERE key IS NOT NULL
),
filtered_app_logs_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        key,
        service_lower,
        clean_service,
        service_upper,
        resolved_key,
        INITCAP(TRIM(level)) AS level_title,
        UPPER(level) AS level_upper
    FROM filtered_app_logs
    WHERE key IS NOT NULL
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        key,
        service_lower,
        clean_service,
        service_upper,
        resolved_key,
        level_title,
        level_upper,
        LENGTH(key) AS key_length
    FROM filtered_app_logs_2
    WHERE key IS NOT NULL
),
totals AS (
    -- Grouped metrics
    SELECT
        key,
        CASE WHEN key_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS key_length_tier,
        ROUND(SUM(key_length) / NULLIF(COUNT(CASE WHEN key = 'region' THEN 1 END), 0), 2) AS key_length_per_region,
        ROUND(AVG(key_length), 2) AS avg_key_length,
        COUNT(DISTINCT key) AS distinct_keys,
        COUNT(*) FILTER (WHERE key = 'region') AS region_rows,
        SUM(CASE WHEN key = 'region' THEN key_length ELSE 0 END) AS region_key_length,
        RANK() OVER (PARTITION BY key ORDER BY COUNT(*) FILTER (WHERE key = 'region') DESC) AS region_rows_rank
    FROM cleaned_app_logs
    GROUP BY key, CASE WHEN key_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    key,
    key_length_tier,
    key_length_per_region,
    avg_key_length,
    distinct_keys,
    region_key_length
FROM totals;
