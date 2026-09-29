WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        status_code,
        attr_key,
        v,
        MIN(latency_ms) OVER w_ordered AS running_min_latency_ms,
        AVG(latency_ms) OVER w_ordered AS running_avg_latency_ms,
        SUM(latency_ms) OVER w_ordered AS running_sum_latency_ms,
        COUNT(latency_ms) OVER w_ordered AS running_count_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms, id, attr_key) AS row_seq
    FROM lines
    WHERE host IN ('h1', 'h4')
    WINDOW w_ordered AS (PARTITION BY host ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        running_min_latency_ms,
        running_avg_latency_ms,
        running_sum_latency_ms,
        running_count_latency_ms,
        LAG(running_avg_latency_ms, 1) OVER (ORDER BY status_code, id, attr_key) AS prev_running_avg_latency_ms,
        NTILE(5) OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS status_code_bucket,
        AVG(running_count_latency_ms) OVER w_grp AS running_count_latency_ms_avg_2_prior,
        AVG(status_code) OVER (PARTITION BY host) AS host_avg_status_code,
        LAG(running_sum_latency_ms, 1) OVER (PARTITION BY attr_key ORDER BY running_min_latency_ms, id, attr_key) AS prev_running_sum_latency_ms
    FROM scored
    WINDOW w_grp AS (PARTITION BY host ORDER BY status_code, id, attr_key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        running_min_latency_ms,
        running_sum_latency_ms,
        running_count_latency_ms,
        status_code_bucket,
        host_avg_status_code,
        LAST_VALUE(attr_key) IGNORE NULLS OVER (ORDER BY prev_running_sum_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
        SUM(running_count_latency_ms_avg_2_prior) OVER (PARTITION BY attr_key ORDER BY running_count_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_latency_ms_avg_2_prior
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        running_sum_latency_ms,
        status_code_bucket,
        last_known_attr_key,
        SUM(running_min_latency_ms) OVER (PARTITION BY attr_key ORDER BY running_count_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_min_latency_ms,
        AVG(running_min_latency_ms) OVER (ORDER BY running_min_latency_ms, id, attr_key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_min_latency_ms_avg_6_prior
    FROM with_windows
)
SELECT
    id,
    attr_key,
    running_sum_latency_ms,
    status_code_bucket,
    running_running_min_latency_ms
FROM metrics
ORDER BY running_running_min_latency_ms DESC NULLS LAST
