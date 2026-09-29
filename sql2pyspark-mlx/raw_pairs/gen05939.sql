WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
parsed AS (
    SELECT
        service,
        level,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
),
cleaned AS (
    SELECT *, LEAST(status_code_high, status_code) AS least_status_code_high_status_code
    FROM parsed
    WHERE status_code < 292
),
base AS (
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM cleaned
    WHERE status_code_high = 2
),
metrics AS (
    SELECT
        flag_bit2,
        service_title,
        CUME_DIST() OVER (ORDER BY flag_bit2 NULLS LAST) AS flag_bit2_cume_dist,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY level) AS level_stddev_samp_status_code_high
    FROM base
    WHERE least_status_code_high_status_code < 46
),
valid AS (
    SELECT *, level_stddev_samp_status_code_high - flag_bit2 AS level_stddev_samp_status_code_high_minus_flag_bit2
    FROM metrics
    WHERE flag_bit2 >= 1
),
metrics_app_logs AS (
    SELECT
        flag_bit2,
        level_stddev_samp_status_code_high,
        CUME_DIST() OVER (ORDER BY flag_bit2_cume_dist DESC NULLS FIRST) AS flag_bit2_cume_dist_cume_dist,
        DENSE_RANK() OVER (ORDER BY flag_bit2 DESC NULLS FIRST) AS flag_bit2_dense_rank,
        DENSE_RANK() OVER (ORDER BY level_stddev_samp_status_code_high_minus_flag_bit2) AS level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY level_stddev_samp_status_code_high_minus_flag_bit2) AS level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank
    FROM valid
),
daily AS (
    SELECT
        CASE WHEN flag_bit2_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS flag_bit2_dense_rank_tier,
        CASE WHEN level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier,
        PERCENTILE_APPROX(flag_bit2, 0.5) AS flag_bit2_p50,
        APPROX_PERCENTILE(level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank, 0.9) AS level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank_p90,
        COUNT(*) AS num_rows,
        COUNT(*) AS row_count
    FROM metrics_app_logs
    GROUP BY CASE WHEN flag_bit2_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING COUNT(*) >= 1
)
SELECT
    flag_bit2_dense_rank_tier,
    level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier,
    flag_bit2_p50,
    level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank_p90,
    num_rows,
    row_count
FROM daily
ORDER BY level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier
