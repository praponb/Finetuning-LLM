WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY level, key ORDER BY status_code) AS status_code_rank,
        CORR(latency_ms, status_code) OVER (PARTITION BY level, key) AS corr_latency_ms_status_code
    FROM exploded
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code_rank,
        COUNT(status_code_rank) OVER (PARTITION BY level) AS level_count_status_code_rank
    FROM with_windows
    WHERE corr_latency_ms_status_code BETWEEN -0.6 AND -0.5
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        level_count_status_code_rank,
        PERCENT_RANK() OVER w_hist AS latency_ms_percent_rank,
        COUNT(latency_ms) OVER w_hist_2 AS level_count_latency_ms
    FROM enriched
    WINDOW w_hist AS (PARTITION BY level ORDER BY latency_ms DESC NULLS LAST), w_hist_2 AS (PARTITION BY level)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(level_count_status_code_rank * 0.9, 2) AS level_count_status_code_rank_scaled
    FROM scored
    WHERE level = 'ERROR'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        level,
        level_count_status_code_rank,
        level_count_latency_ms,
        level_count_status_code_rank_scaled,
        LEAST(level_count_status_code_rank_scaled, level_count_latency_ms) AS least_level_count_status_code_rank_scaled_level_count_latency_ms
    FROM filtered
    WHERE level_count_latency_ms > 13
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        level,
        level_count_status_code_rank_scaled,
        AVG(least_level_count_status_code_rank_scaled_level_count_latency_ms) OVER (PARTITION BY level) AS level_avg_least_level_count_status_code_rank_scaled_level_count_latency_ms
    FROM valid
)
SELECT level_count_status_code_rank_scaled
FROM scored_app_logs
ORDER BY level_count_status_code_rank_scaled DESC
