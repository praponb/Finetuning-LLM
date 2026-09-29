WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(service)) AS service_title,
        CONCAT_WS('-', service, attr_key) AS service_attr_key_key,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM unpacked
    WHERE status_code < 383
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        host,
        status_code,
        status_code_high,
        is_flagged,
        service_title,
        service_attr_key_key,
        attr_key_filled,
        is_flagged - status_code AS is_flagged_minus_status_code
    FROM base
    WHERE attr_key IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY host ORDER BY is_flagged_minus_status_code DESC NULLS FIRST) AS is_flagged_minus_status_code_rank,
        PERCENT_RANK() OVER w_running AS is_flagged_minus_status_code_percent_rank,
        DENSE_RANK() OVER (ORDER BY is_flagged_minus_status_code DESC) AS is_flagged_minus_status_code_dense_rank
    FROM valid
    WHERE is_flagged_minus_status_code BETWEEN 48 AND 56
    WINDOW w_running AS (PARTITION BY host ORDER BY is_flagged_minus_status_code DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    host,
    APPROX_PERCENTILE(is_flagged_minus_status_code_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_minus_status_code_dense_rank_quartiles,
    ROUND(SUM(status_code_high) / NULLIF(COUNT(CASE WHEN host = 'h1' THEN 1 END), 0), 2) AS status_code_high_per_h1,
    SUM(CASE WHEN host = 'h4' THEN is_flagged_minus_status_code_rank ELSE 0 END) AS h4_is_flagged_minus_status_code_rank
FROM enriched
GROUP BY host
ORDER BY status_code_high_per_h1 DESC
