WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(attr_key)) AS attr_key_title
    FROM lines
    WHERE attr_key IS DISTINCT FROM 'version'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        attr_key_title,
        GREATEST(latency_ms, status_code) AS greatest_latency_ms_status_code,
        CASE WHEN attr_key = 'region' THEN 1 ELSE 0 END AS is_region
    FROM valid
    WHERE attr_key IN ('version', 'region', 'trace')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        host,
        level,
        status_code,
        attr_key,
        v,
        attr_key_title,
        is_region,
        LOWER(level) AS level_lower
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'version'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(status_code) OVER (PARTITION BY attr_key, level) AS attr_key_min_status_code,
        RANK() OVER (ORDER BY status_code) AS status_code_rank
    FROM base
    WHERE attr_key IN ('region', 'trace', 'version')
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_region >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_region_tier,
    CASE WHEN attr_key_min_status_code >= 400 THEN 'HIGH' ELSE 'LOW' END AS attr_key_min_status_code_tier,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    COUNT(*) FILTER (WHERE level = 'WARN') AS warn_rows,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    SUM(CASE WHEN level = 'WARN' THEN is_region ELSE 0 END) AS warn_is_region,
    ROUND(SUM(is_region) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS is_region_per_debug
FROM enriched
GROUP BY CASE WHEN is_region >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN attr_key_min_status_code >= 400 THEN 'HIGH' ELSE 'LOW' END
ORDER BY warn_is_region DESC
