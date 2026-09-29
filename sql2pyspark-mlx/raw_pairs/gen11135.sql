WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_dense_rank,
        AVG(status_code) OVER (PARTITION BY host ORDER BY status_code, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_avg_6_prior,
        NTILE(4) OVER (ORDER BY status_code, id, k) AS status_code_bucket
    FROM exploded
    WHERE status_code = 267
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_avg_6_prior NULLS FIRST, id, k) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
deduped_app_logs AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        status_code_dense_rank,
        status_code_avg_6_prior,
        status_code_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code_dense_rank DESC, id, k) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_grp AS status_code_bucket_bucket,
        PERCENT_RANK() OVER w_part AS status_code_dense_rank_percent_rank,
        AVG(status_code_dense_rank) OVER w_recent AS status_code_dense_rank_avg_6,
        SUM(status_code_avg_6_prior) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_avg_6_prior
    FROM deduped_app_logs
    WINDOW w_grp AS (ORDER BY status_code_bucket, id, k), w_part AS (PARTITION BY host ORDER BY status_code_dense_rank NULLS FIRST), w_recent AS (PARTITION BY level ORDER BY status_code, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code_bucket_bucket) OVER (PARTITION BY service ORDER BY status_code_dense_rank_avg_6, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_bucket_bucket_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY status_code_dense_rank NULLS FIRST) AS status_code_dense_rank_dense_rank
    FROM metrics_app_logs
),
metrics_app_logs_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY status_code_dense_rank_avg_6 DESC) AS status_code_dense_rank_avg_6_percent_rank,
        ROUND(cumulative_status_code_avg_6_prior / SUM(cumulative_status_code_avg_6_prior) OVER (PARTITION BY service), 4) AS cumulative_status_code_avg_6_prior_share,
        AVG(status_code_dense_rank) OVER (PARTITION BY service) AS service_avg_status_code_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY status_code_dense_rank_avg_6 DESC NULLS FIRST) AS status_code_dense_rank_avg_6_percent_rank_2,
        LAG(status_code_dense_rank_percent_rank, 1) OVER (PARTITION BY k ORDER BY status_code_dense_rank_avg_6, id, k) AS prev_status_code_dense_rank_percent_rank
    FROM enriched
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_code_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END AS status_code_bucket_tier,
        k,
        service,
        COUNT_IF(level = 'WARN') AS warn_events,
        SUM(CASE WHEN host = 'h4' THEN status_code_bucket_bucket ELSE 0 END) AS h4_status_code_bucket_bucket,
        MAX(status_code) FILTER (WHERE host = 'h2') AS h2_max_status_code,
        COUNT(*) FILTER (WHERE level = 'INFO') AS info_rows,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN host = 'h4' THEN status_code_bucket_bucket ELSE 0 END) DESC) AS h4_status_code_bucket_bucket_dense_rank
    FROM metrics_app_logs_2
    GROUP BY CASE WHEN status_code_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END, k, service
)
SELECT
    status_code_bucket_tier,
    k,
    service,
    h4_status_code_bucket_bucket
FROM daily
ORDER BY h4_status_code_bucket_bucket DESC NULLS FIRST
