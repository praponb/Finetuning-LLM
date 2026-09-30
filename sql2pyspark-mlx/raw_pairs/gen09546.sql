WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(status_code) OVER (PARTITION BY host ORDER BY status_code, id, k) AS prev_status_code,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code, id, k) AS event_seq,
        SUM(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code
    FROM flattened
    WHERE k = 'trace'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY prev_status_code DESC NULLS FIRST) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        k,
        prev_status_code,
        running_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY host ORDER BY running_status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        LAST_VALUE(prev_status_code) IGNORE NULLS OVER w_part AS last_known_prev_status_code,
        ROUND(running_status_code / SUM(running_status_code) OVER (PARTITION BY service, k), 4) AS running_status_code_share,
        SUM(running_status_code) OVER (PARTITION BY host, service ORDER BY dedup_rank, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_status_code
    FROM deduped
    WINDOW w_part AS (PARTITION BY k ORDER BY prev_status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(last_known_prev_status_code * 0.9, 2) AS last_known_prev_status_code_adjusted
    FROM scored
    WHERE k IS NOT NULL
      AND running_status_code_share >= 0.73
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        k,
        prev_status_code,
        running_status_code,
        status_code_share,
        first_known_k,
        running_status_code_share,
        running_running_status_code,
        MAX(last_known_prev_status_code_adjusted) OVER (PARTITION BY host ORDER BY running_running_status_code, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_last_known_prev_status_code_adjusted,
        MIN(last_known_prev_status_code_adjusted) OVER (PARTITION BY host ORDER BY running_running_status_code, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_last_known_prev_status_code_adjusted,
        SUM(last_known_prev_status_code_adjusted) OVER (PARTITION BY host ORDER BY running_running_status_code, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_last_known_prev_status_code_adjusted,
        NTILE(4) OVER (PARTITION BY k ORDER BY running_running_status_code, id, k) AS running_running_status_code_bucket
    FROM cleaned
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        k,
        prev_status_code,
        running_status_code,
        status_code_share,
        first_known_k,
        running_status_code_share,
        running_running_status_code,
        last3_max_last_known_prev_status_code_adjusted,
        last3_min_last_known_prev_status_code_adjusted,
        last3_sum_last_known_prev_status_code_adjusted,
        running_running_status_code_bucket,
        ROUND(running_status_code * 0.9, 2) AS running_status_code_adjusted
    FROM enriched_app_logs
    WHERE k IS NOT NULL
      AND last3_sum_last_known_prev_status_code_adjusted BETWEEN 170 AND 380
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_share >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_tier,
    host,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    APPROX_PERCENTILE(running_status_code_share, 0.9) AS running_status_code_share_p90,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(running_running_status_code_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_running_status_code_bucket_quartiles,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code_share >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(host)) DESC) AS num_hosts_dense_rank
FROM filtered
GROUP BY CASE WHEN status_code_share >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END, host
