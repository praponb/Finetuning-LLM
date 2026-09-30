WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        attr_value,
        AVG(status_code) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_avg_6_prior,
        LAG(status_code, 2) OVER (ORDER BY status_code, id, k) AS prev_status_code
    FROM exploded
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        k,
        prev_status_code,
        SUM(prev_status_code) OVER (PARTITION BY level ORDER BY status_code_avg_6_prior, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_status_code,
        SUM(status_code) OVER (ORDER BY prev_status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        NTILE(5) OVER (ORDER BY prev_status_code, id, k) AS prev_status_code_bucket,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', log_ts), k ORDER BY prev_status_code DESC, id, k) AS prev_status_code_bucket_2
    FROM with_windows
    WHERE status_code_avg_6_prior BETWEEN 350 AND 358
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY prev_status_code_bucket_2 DESC, id, k) AS row_num
        FROM scored
    ) t
    WHERE row_num = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_status_code_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS prev_status_code_bucket_tier,
        level,
        SUM(CASE WHEN level = 'INFO' THEN cumulative_prev_status_code ELSE 0 END) AS info_cumulative_prev_status_code,
        ROUND(SUM(prev_status_code_bucket) / NULLIF(COUNT(CASE WHEN k = 'region' THEN 1 END), 0), 2) AS prev_status_code_bucket_per_region,
        COUNT_IF(k = 'region') AS region_events
    FROM ranked
    GROUP BY CASE WHEN prev_status_code_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, level
)
SELECT
    prev_status_code_bucket_tier,
    level,
    info_cumulative_prev_status_code,
    region_events
FROM daily;
