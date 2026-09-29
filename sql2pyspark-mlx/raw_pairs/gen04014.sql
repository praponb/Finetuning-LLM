WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        key,
        value,
        LEAD(latency_ms, 1, 0) OVER (PARTITION BY key ORDER BY status_code ASC, id ASC, key ASC) AS next_latency_ms,
        CORR(status_code, latency_ms) OVER (PARTITION BY service) AS corr_status_code_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, key), 4) AS latency_ms_share
    FROM exploded
    WHERE service = 'search'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(latency_ms_share, 1) OVER (PARTITION BY service, key ORDER BY next_latency_ms ASC, id ASC, key ASC) AS prev_latency_ms_share,
        MAX(latency_ms) OVER (PARTITION BY service) AS service_max_latency_ms,
        NTILE(10) OVER (PARTITION BY service ORDER BY corr_status_code_latency_ms DESC, id ASC, key ASC) AS corr_status_code_latency_ms_bucket,
        AVG(latency_ms_share) OVER (PARTITION BY key) AS key_avg_latency_ms_share,
        LAST_VALUE(latency_ms_share) IGNORE NULLS OVER (PARTITION BY key, service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms_share
    FROM enriched
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        key,
        next_latency_ms,
        corr_status_code_latency_ms,
        latency_ms_share,
        service_max_latency_ms,
        corr_status_code_latency_ms_bucket,
        LAST_VALUE(prev_latency_ms_share, TRUE) OVER (PARTITION BY key ORDER BY key_avg_latency_ms_share ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_latency_ms_share,
        AVG(key_avg_latency_ms_share) OVER (PARTITION BY key ORDER BY corr_status_code_latency_ms ASC, id ASC, key ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS key_avg_latency_ms_share_avg_5,
        NTILE(4) OVER (ORDER BY next_latency_ms DESC, id ASC, key ASC) AS next_latency_ms_bucket,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC) AS event_seq,
        NTILE(10) OVER w_grp AS key_avg_latency_ms_share_bucket
    FROM scored
    WINDOW w_grp AS (ORDER BY key_avg_latency_ms_share DESC, id ASC, key ASC)
),
scored_app_logs_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        key,
        next_latency_ms,
        service_max_latency_ms,
        last_known_prev_latency_ms_share,
        key_avg_latency_ms_share_avg_5,
        next_latency_ms_bucket,
        key_avg_latency_ms_share_bucket,
        LEAD(next_latency_ms_bucket) OVER (PARTITION BY key ORDER BY corr_status_code_latency_ms ASC, id ASC, key ASC) AS next_next_latency_ms_bucket,
        LEAD(key_avg_latency_ms_share_bucket) OVER (PARTITION BY key ORDER BY corr_status_code_latency_ms ASC, id ASC, key ASC) AS next_key_avg_latency_ms_share_bucket,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY corr_status_code_latency_ms ASC, id ASC, key ASC) AS seq_no
    FROM scored_app_logs
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        key,
        next_latency_ms,
        service_max_latency_ms,
        last_known_prev_latency_ms_share,
        key_avg_latency_ms_share_avg_5,
        key_avg_latency_ms_share_bucket,
        next_next_latency_ms_bucket,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY next_next_latency_ms_bucket DESC NULLS LAST, id ASC, key ASC) AS latest_rank
        FROM scored_app_logs_2
    ) x
    WHERE latest_rank = 1
),
scored_app_logs_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY service_max_latency_ms DESC, id ASC, key ASC) AS row_seq,
        AVG(seq_no) OVER (PARTITION BY key) AS key_avg_seq_no
    FROM ranked
)
-- Aggregate with conditional metrics
SELECT
    key,
    key_avg_seq_no,
    SIZE(COLLECT_SET(key)) AS num_keys,
    ROUND(SUM(next_next_latency_ms_bucket) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS next_next_latency_ms_bucket_per_trace,
    APPROX_PERCENTILE(next_latency_ms, 0.5) AS next_latency_ms_p50
FROM scored_app_logs_3
GROUP BY GROUPING SETS ((key, key_avg_seq_no), (key))
ORDER BY key DESC
