WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
scored AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        status_code,
        v,
        MIN(latency_ms) OVER (PARTITION BY YEAR(log_ts), k) AS log_ts_year_min_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY level, k ORDER BY latency_ms) AS latency_ms_percent_rank
    FROM lines
    WHERE TO_DATE(log_ts) < '2023-03-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        log_ts,
        status_code,
        v,
        log_ts_year_min_latency_ms,
        latency_ms_percent_rank,
        TO_DATE(log_ts) AS log_ts_date,
        COALESCE(latency_ms_percent_rank, 0) AS latency_ms_percent_rank_filled
    FROM scored
    WHERE log_ts >= TIMESTAMP '2025-06-01 07:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(log_ts_year_min_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS log_ts_year_min_latency_ms_sum_2h,
        RANK() OVER w_ordered AS latency_ms_percent_rank_rank,
        PERCENT_RANK() OVER (ORDER BY status_code) AS status_code_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY YEAR(log_ts_date)), 4) AS status_code_share
    FROM valid
    WINDOW w_ordered AS (ORDER BY latency_ms_percent_rank DESC)
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        log_ts,
        status_code,
        v,
        log_ts_year_min_latency_ms,
        latency_ms_percent_rank,
        log_ts_date,
        latency_ms_percent_rank_filled,
        log_ts_year_min_latency_ms_sum_2h,
        latency_ms_percent_rank_rank,
        status_code_share,
        COALESCE(status_code_share, 0) AS status_code_share_filled,
        log_ts_year_min_latency_ms_sum_2h * log_ts_year_min_latency_ms AS log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms
    FROM enriched
    WHERE status_code != 277
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        status_code,
        v,
        log_ts_year_min_latency_ms,
        log_ts_date,
        latency_ms_percent_rank_filled,
        log_ts_year_min_latency_ms_sum_2h,
        latency_ms_percent_rank_rank,
        status_code_share,
        log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms,
        RANK() OVER (ORDER BY log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms DESC) AS log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank,
        SUM(latency_ms_percent_rank_filled) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_percent_rank_filled_sum_2h,
        RANK() OVER (ORDER BY latency_ms_percent_rank_rank DESC) AS latency_ms_percent_rank_rank_rank
    FROM valid_app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    DATE_TRUNC('QUARTER', log_ts_date) AS quarter_start,
    MIN(status_code) AS min_status_code,
    BIT_OR(status_code) AS status_code_bit_or,
    ROUND(AVG(log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank), 2) AS avg_log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank,
    AVG(log_ts_year_min_latency_ms_sum_2h) AS avg_log_ts_year_min_latency_ms_sum_2h,
    PERCENTILE_APPROX(log_ts_year_min_latency_ms_sum_2h, 0.5) AS log_ts_year_min_latency_ms_sum_2h_p50,
    ROUND(AVG(latency_ms_percent_rank_rank), 2) AS avg_latency_ms_percent_rank_rank,
    GROUPING_ID() AS grouping_id
FROM enriched_app_logs
GROUP BY CUBE (DATE_TRUNC('HOUR', log_ts), DATE_TRUNC('DAY', log_ts), DATE_TRUNC('QUARTER', log_ts_date))
