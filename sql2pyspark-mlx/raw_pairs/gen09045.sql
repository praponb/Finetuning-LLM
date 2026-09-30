WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        v,
        CASE WHEN host = 'h1' THEN 1 ELSE 0 END AS is_h1,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM flattened
    WHERE level = 'WARN'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(host, 1, 3) AS host_prefix,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM filtered
    WHERE status_code != 394
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        status_code,
        attr_key,
        attr_key_clean,
        resolved_attr_key,
        DENSE_RANK() OVER w_running AS status_code_dense_rank,
        MIN(is_h1) OVER w_running_2 AS host_min_is_h1
    FROM base
    WHERE host IN ('h3', 'h4', 'h2')
    WINDOW w_running AS (PARTITION BY host ORDER BY status_code DESC), w_running_2 AS (PARTITION BY host)
)
SELECT
    level,
    attr_key,
    attr_key_clean,
    resolved_attr_key,
    status_code_dense_rank,
    host_min_is_h1
FROM scored
ORDER BY level DESC NULLS FIRST;
