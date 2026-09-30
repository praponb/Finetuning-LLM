WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        status_code,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist,
        CUME_DIST() OVER (PARTITION BY MONTH(log_ts) ORDER BY is_flagged DESC NULLS FIRST) AS is_flagged_cume_dist
    FROM parsed
    WHERE TO_DATE(log_ts) >= '2023-08-12'
),
metrics_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        status_code,
        is_flagged,
        status_code_high,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('week', log_ts) ORDER BY is_flagged_cume_dist NULLS FIRST) AS is_flagged_cume_dist_cume_dist,
        AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m,
        CUME_DIST() OVER (ORDER BY status_code_cume_dist DESC NULLS FIRST) AS status_code_cume_dist_cume_dist
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_dense_rank,
        RANK() OVER w_grp AS status_code_cume_dist_cume_dist_rank
    FROM metrics_app_logs
    WINDOW w_grp AS (ORDER BY status_code_cume_dist_cume_dist DESC NULLS LAST)
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_1h,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m,
        CUME_DIST() OVER (ORDER BY is_flagged DESC NULLS FIRST) AS is_flagged_cume_dist
    FROM scored
)
SELECT
    id,
    log_ts,
    status_code,
    status_code_high,
    status_code_cume_dist_cume_dist,
    status_code_cume_dist_cume_dist_rank,
    is_flagged_cume_dist
FROM scored_app_logs
ORDER BY is_flagged_cume_dist
