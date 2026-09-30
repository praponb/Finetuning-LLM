WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM exploded
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE log_ts < '2023-07-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(status_code) OVER (PARTITION BY attr_key) AS attr_key_min_status_code,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_avg_1h,
        PERCENT_RANK() OVER w_running AS status_code_percent_rank,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code ASC) AS status_code_cume_dist,
        DENSE_RANK() OVER w_running AS status_code_dense_rank
    FROM kept_rows
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY status_code DESC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(status_code_avg_1h) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_avg_1h_count_1h,
        SUM(status_code_avg_1h) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_avg_1h_sum_2h
    FROM enriched
    WHERE attr_key_min_status_code = 424
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        log_ts,
        status_code,
        value,
        status_code_percent_rank,
        status_code_avg_1h_count_1h,
        status_code_avg_1h_sum_2h,
        CUME_DIST() OVER w_running AS status_code_avg_1h_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(log_ts) ORDER BY status_code_dense_rank DESC NULLS LAST) AS status_code_dense_rank_dense_rank,
        RANK() OVER (ORDER BY status_code_dense_rank ASC) AS status_code_dense_rank_rank,
        MAX(status_code_percent_rank) OVER (PARTITION BY MONTH(log_ts), attr_key) AS log_ts_month_max_status_code_percent_rank
    FROM metrics
    WINDOW w_running AS (ORDER BY status_code_avg_1h ASC NULLS LAST)
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_ordered AS status_code_rank,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h,
        PERCENT_RANK() OVER (ORDER BY status_code_dense_rank_dense_rank DESC NULLS LAST) AS status_code_dense_rank_dense_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY log_ts_month_max_status_code_percent_rank DESC NULLS LAST) AS log_ts_month_max_status_code_percent_rank_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY MONTH(log_ts)), 4) AS status_code_share
    FROM enriched_app_logs
    WINDOW w_ordered AS (ORDER BY status_code DESC)
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_dense_rank_dense_rank_percent_rank >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_dense_rank_dense_rank_percent_rank_tier,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    BIT_AND(status_code) AS status_code_bit_and,
    AVG(status_code_dense_rank_dense_rank_percent_rank) AS avg_status_code_dense_rank_dense_rank_percent_rank
FROM metrics_app_logs
GROUP BY CASE WHEN status_code_dense_rank_dense_rank_percent_rank >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END;
