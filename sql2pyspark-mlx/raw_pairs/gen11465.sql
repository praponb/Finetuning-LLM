WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        log_ts,
        status_code,
        value,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code != 280
),
enriched AS (
    -- Ranking and gap features
    SELECT
        log_ts,
        value,
        DENSE_RANK() OVER (ORDER BY status_code DESC) AS status_code_dense_rank,
        MAX(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_max_2h,
        COUNT(flag_bit4) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS flag_bit4_count_30m
    FROM parsed
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        value,
        status_code_high_max_2h,
        MAX(status_code_high_max_2h) OVER w_hist AS status_code_high_max_2h_max_15m,
        CUME_DIST() OVER (ORDER BY status_code_dense_rank NULLS LAST) AS status_code_dense_rank_cume_dist,
        MAX(status_code_dense_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_dense_rank_max_30m
    FROM enriched
    WINDOW w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        status_code_high_max_2h / NULLIF(status_code_high_max_2h_max_15m, 0) AS status_code_high_max_2h_per_status_code_high_max_2h_max_15m,
        WEEKOFYEAR(log_ts) AS log_ts_weekofyear
    FROM with_windows
    WHERE log_ts > DATE '2025-06-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        status_code_high_max_2h,
        status_code_high_max_2h_max_15m,
        status_code_dense_rank_cume_dist,
        log_ts_weekofyear,
        DENSE_RANK() OVER (ORDER BY status_code_dense_rank_cume_dist NULLS LAST) AS status_code_dense_rank_cume_dist_dense_rank,
        RANK() OVER (ORDER BY status_code_high_max_2h_per_status_code_high_max_2h_max_15m NULLS FIRST) AS status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank,
        CUME_DIST() OVER (ORDER BY status_code_dense_rank_cume_dist DESC NULLS FIRST) AS status_code_dense_rank_cume_dist_cume_dist
    FROM cleaned
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank / NULLIF(status_code_high_max_2h_max_15m, 0) AS status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank_per_status_code_high_max_2h_max_15m
    FROM scored
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 07:00:00')
)
-- Window metrics over several different partitions
SELECT
    *,
    MAX(status_code_high_max_2h_max_15m) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_max_2h_max_15m_max_1h,
    SUM(status_code_dense_rank_cume_dist_dense_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_dense_rank_cume_dist_dense_rank_sum_30m
FROM filtered;
