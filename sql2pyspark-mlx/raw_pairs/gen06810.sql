WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
with_windows AS (
    SELECT
        id,
        host,
        level,
        k,
        ROW_NUMBER() OVER (ORDER BY status_code DESC, id, k) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) AS event_seq,
        LAG(status_code, 1) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_status_code
    FROM flattened
    WHERE log_ts < '2024-07-01'
),
base AS (
    SELECT
        *,
        ROUND(prev_status_code, 1) AS prev_status_code_rounded,
        COALESCE(k, 'NONE') AS k_filled
    FROM with_windows
    WHERE k IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MIN(prev_status_code_rounded) OVER (PARTITION BY level, host ORDER BY row_seq, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_prev_status_code_rounded,
        AVG(prev_status_code_rounded) OVER (PARTITION BY level, host ORDER BY row_seq, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_prev_status_code_rounded,
        COUNT(prev_status_code_rounded) OVER (PARTITION BY level, host ORDER BY row_seq, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_prev_status_code_rounded
    FROM base
),
metrics_app_logs AS (
    SELECT
        id,
        host,
        level,
        k,
        event_seq,
        NTILE(3) OVER w_recent AS prev_status_code_bucket,
        AVG(prev_status_code_rounded) OVER (PARTITION BY level ORDER BY last3_avg_prev_status_code_rounded, id, k ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS prev_status_code_rounded_avg_5_prior
    FROM metrics
    WINDOW w_recent AS (PARTITION BY level ORDER BY prev_status_code DESC, id, k)
),
valid AS (
    SELECT *, GREATEST(prev_status_code_bucket, prev_status_code_rounded_avg_5_prior) AS greatest_prev_status_code_bucket_prev_status_code_rounded_avg_5_prior
    FROM metrics_app_logs
    WHERE k IS NOT NULL
)
SELECT
    host,
    APPROX_PERCENTILE(prev_status_code_bucket, 0.5) AS prev_status_code_bucket_p50,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    GROUPING(host) AS gid
FROM valid
GROUP BY CUBE (host)
ORDER BY host DESC
