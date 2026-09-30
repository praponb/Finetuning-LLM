WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY host ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        NTILE(4) OVER (PARTITION BY k ORDER BY status_code DESC, id, k) AS status_code_bucket
    FROM flattened
    WHERE host IN ('h3', 'h4', 'h1')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code DESC, id, k) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
metrics_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(status_code_bucket) OVER (PARTITION BY level ORDER BY status_code, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_status_code_bucket,
        MAX(status_code_bucket) OVER (PARTITION BY level ORDER BY status_code, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_status_code_bucket,
        SUM(status_code_bucket) OVER (PARTITION BY level ORDER BY status_code, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_status_code_bucket,
        AVG(status_code_bucket) OVER (PARTITION BY service ORDER BY dedup_rank, id, k ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_bucket_avg_3_prior
    FROM ranked
),
metrics_app_logs_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last4_sum_status_code_bucket) OVER (PARTITION BY k ORDER BY last4_max_status_code_bucket, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last4_sum_status_code_bucket,
        NTILE(5) OVER (PARTITION BY service ORDER BY status_code_bucket DESC, id, k) AS status_code_bucket_bucket,
        LAG(last4_sum_status_code_bucket, 1) OVER (PARTITION BY service ORDER BY last4_min_status_code_bucket, id, k) AS prev_last4_sum_status_code_bucket
    FROM metrics_app_logs
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(status_code_bucket_bucket) OVER w_grp AS running_min_status_code_bucket_bucket,
        COUNT(status_code_bucket_bucket) OVER w_grp AS running_count_status_code_bucket_bucket,
        AVG(status_code_bucket_bucket) OVER w_grp AS running_avg_status_code_bucket_bucket,
        AVG(prev_last4_sum_status_code_bucket) OVER w_hist AS prev_last4_sum_status_code_bucket_avg_5
    FROM metrics_app_logs_2
    WINDOW w_grp AS (PARTITION BY level, host ORDER BY running_last4_sum_status_code_bucket, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY status_code_bucket_avg_3_prior, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
metrics_app_logs_3 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        host,
        level,
        k,
        last4_min_status_code_bucket,
        last4_sum_status_code_bucket,
        prev_last4_sum_status_code_bucket,
        running_avg_status_code_bucket_bucket,
        PERCENT_RANK() OVER (ORDER BY running_min_status_code_bucket_bucket DESC NULLS FIRST) AS running_min_status_code_bucket_bucket_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code, id, k) AS row_seq
    FROM scored
)
-- Grouped metrics
SELECT
    level,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count
FROM metrics_app_logs_3
GROUP BY level
