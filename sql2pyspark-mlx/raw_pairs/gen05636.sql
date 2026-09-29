WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        level,
        key,
        v,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE key NOT IN ('region', 'version')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN is_flagged >= 1 THEN 'High' WHEN is_flagged >= 0 THEN 'Medium' ELSE 'Low' END AS is_flagged_tier,
        INITCAP(TRIM(key)) AS key_title,
        COALESCE(key, 'NONE') AS key_filled
    FROM parsed
    WHERE key IS NOT NULL
      AND level = 'DEBUG'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY is_flagged ASC) AS is_flagged_percent_rank,
        CUME_DIST() OVER (ORDER BY status_code_high ASC NULLS FIRST) AS status_code_high_cume_dist
    FROM base
)
SELECT
    key,
    v,
    is_flagged,
    status_code_high,
    is_flagged_tier,
    key_title,
    key_filled,
    is_flagged_percent_rank,
    status_code_high_cume_dist,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS key_zone
FROM metrics
ORDER BY status_code_high ASC NULLS FIRST, is_flagged DESC
