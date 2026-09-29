WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_key,
        v,
        INITCAP(TRIM(host)) AS host_title
    FROM flattened
    WHERE service IN ('billing', 'search', 'checkout')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM filtered
    WHERE attr_key NOT IN ('version', 'region')
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END AS is_trace,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END AS is_trace_2
    FROM valid
    WHERE attr_key IS NOT NULL
      AND attr_key = 'trace'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY is_trace NULLS FIRST) AS is_trace_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY is_trace_2 DESC NULLS LAST) AS is_trace_2_percent_rank
    FROM valid_app_logs
    WHERE is_trace BETWEEN 57 AND 58
),
report AS (
    SELECT
        id,
        attr_key,
        v,
        host_title,
        clean_attr_key,
        is_trace_rank,
        is_trace_2_percent_rank
    FROM scored
    ORDER BY attr_key
)
SELECT
    id,
    attr_key,
    v,
    host_title,
    is_trace_rank,
    is_trace_2_percent_rank
FROM report
ORDER BY is_trace_2_percent_rank DESC NULLS FIRST, id DESC;
