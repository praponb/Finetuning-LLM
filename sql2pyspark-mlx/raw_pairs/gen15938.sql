WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        k,
        attr_value,
        UPPER(level) AS level_upper,
        UPPER(k) AS k_upper,
        COALESCE(k, 'NONE') AS k_clean
    FROM lines
    WHERE k IN ('version', 'region')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        level,
        k,
        level_upper,
        CASE WHEN k = 'trace' THEN 1 ELSE 0 END AS is_trace,
        CONCAT_WS('/', service, k) AS service_k_key,
        COALESCE(k, 'NONE') AS k_filled
    FROM filtered
    WHERE k IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        k,
        level_upper,
        is_trace,
        service_k_key,
        COALESCE(is_trace, 0) AS is_trace_filled,
        SUBSTRING(level, 1, 2) AS level_prefix
    FROM valid
    WHERE k IS NOT NULL
      AND k = 'version'
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        k,
        level_upper,
        is_trace,
        is_trace_filled,
        level_prefix,
        LOWER(k) AS k_lower,
        COALESCE(is_trace_filled, 0) AS is_trace_filled_filled
    FROM base
    WHERE is_trace BETWEEN 28 AND 52
)
SELECT
    id,
    k,
    level_upper,
    is_trace_filled,
    k_lower,
    is_trace_filled_filled
FROM filtered_app_logs
ORDER BY k ASC;
