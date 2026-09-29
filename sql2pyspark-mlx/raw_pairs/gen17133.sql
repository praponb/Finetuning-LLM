WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code DESC) status_code_cume_dist,
        RANK() OVER (ORDER BY status_code DESC) status_code_rank
    FROM flattened
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_code,
        key,
        attr_value,
        status_code_cume_dist,
        status_code_rank,
        status_code_cume_dist - status_code_rank status_code_cume_dist_minus_status_code_rank,
        COALESCE(key, 'DIRECT') resolved_key
    FROM with_windows
    WHERE key IS NOT NULL
      AND service = 'billing'
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        status_code,
        key,
        attr_value,
        status_code_cume_dist,
        resolved_key,
        ROUND(status_code * 1.1, 2) adjusted_status_code
    FROM valid
    WHERE key IS DISTINCT FROM 'trace'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(status_code AS STRING) status_code_str,
        CASE WHEN key IN ('version', 'region') THEN 'Tier 1' ELSE 'Other' END key_group
    FROM valid_app_logs
    WHERE key IS NOT DISTINCT FROM 'region'
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        attr_value,
        status_code_cume_dist,
        resolved_key,
        status_code_str,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY status_code_cume_dist DESC NULLS LAST) status_code_cume_dist_dense_rank,
        CUME_DIST() OVER (PARTITION BY key ORDER BY status_code_cume_dist NULLS FIRST) status_code_cume_dist_cume_dist,
        MAX(status_code_cume_dist) OVER (PARTITION BY key) key_max_status_code_cume_dist
    FROM filtered
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_part status_code_cume_dist_cume_dist_dense_rank,
        CUME_DIST() OVER (ORDER BY status_code_cume_dist_cume_dist NULLS LAST) status_code_cume_dist_cume_dist_cume_dist,
        RANK() OVER (ORDER BY key_max_status_code_cume_dist DESC) key_max_status_code_cume_dist_rank,
        PERCENT_RANK() OVER w_grp status_code_cume_dist_dense_rank_percent_rank
    FROM with_windows_app_logs
    WHERE status_code_cume_dist_dense_rank BETWEEN 4 AND 7
    WINDOW w_part AS (ORDER BY status_code_cume_dist_cume_dist DESC NULLS FIRST), w_grp AS (ORDER BY status_code_cume_dist_dense_rank DESC NULLS LAST)
)
-- Grouped metrics
SELECT
    CASE WHEN key_max_status_code_cume_dist_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS key_max_status_code_cume_dist_rank_tier,
    CASE WHEN key_max_status_code_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END AS key_max_status_code_cume_dist_tier,
    CASE WHEN status_code_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_cume_dist_dense_rank_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(status_code_cume_dist_cume_dist_dense_rank, 0.5) AS status_code_cume_dist_cume_dist_dense_rank_p50,
    MIN(status_code_cume_dist_cume_dist) AS min_status_code_cume_dist_cume_dist
FROM scored
GROUP BY CASE WHEN key_max_status_code_cume_dist_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN key_max_status_code_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END;
