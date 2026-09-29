WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        v,
        CONCAT_WS('-', key, level) AS key_level_key,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM flattened
    WHERE key IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        level,
        latency_ms,
        v,
        key_clean,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY level) AS level_stddev_samp_latency_ms,
        CORR(status_code, latency_ms) OVER w_running AS corr_status_code_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_percent_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share_2
    FROM cleaned
    WINDOW w_running AS (PARTITION BY level, key)
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        status_code_percent_rank - latency_ms_share AS status_code_percent_rank_minus_latency_ms_share,
        ROUND(latency_ms_share_2, 1) AS latency_ms_share_2_rounded
    FROM enriched
    WHERE corr_status_code_latency_ms < 0.5
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level_stddev_samp_latency_ms >= 2053 THEN 'High' WHEN level_stddev_samp_latency_ms >= 882 THEN 'Medium' ELSE 'Low' END AS level_stddev_samp_latency_ms_tier
    FROM cleaned_app_logs
    WHERE latency_ms_share_2_rounded < 33
),
summary AS (
    -- Grouped metrics
    SELECT
        level,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list,
        PERCENTILE_APPROX(latency_ms_share_2_rounded, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_share_2_rounded_quartiles
    FROM valid
    GROUP BY level
)
SELECT level, latency_ms_share_2_rounded_quartiles
FROM summary
