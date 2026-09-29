WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
scored AS (
    SELECT
        id,
        log_ts,
        status_code,
        k,
        v,
        LAST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
        MIN(latency_ms) OVER (PARTITION BY k) AS k_min_latency_ms
    FROM lines
    WHERE status_code BETWEEN 381 AND 440
),
with_windows AS (
    SELECT
        id,
        log_ts,
        k,
        last_known_k,
        LAST_VALUE(k_min_latency_ms) IGNORE NULLS OVER (PARTITION BY YEAR(log_ts), k ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k_min_latency_ms,
        FIRST_VALUE(k_min_latency_ms, TRUE) OVER (PARTITION BY k ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k_min_latency_ms,
        PERCENT_RANK() OVER w_running AS k_min_latency_ms_percent_rank,
        SUM(k_min_latency_ms) OVER w_ordered AS cumulative_k_min_latency_ms,
        COUNT(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h
    FROM scored
    WINDOW w_running AS (PARTITION BY k ORDER BY k_min_latency_ms DESC NULLS FIRST), w_ordered AS (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        *,
        ROUND(status_code_count_2h, 1) AS status_code_count_2h_rounded,
        COALESCE(k, 'OTHER') AS k_clean
    FROM with_windows
    WHERE log_ts > DATE '2025-06-01'
      AND k IS DISTINCT FROM 'region'
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
    COUNT(*) FILTER (WHERE k = 'version') AS version_rows_2,
    COUNT(*) FILTER (WHERE k = 'region') AS region_rows,
    APPROX_PERCENTILE(cumulative_k_min_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS cumulative_k_min_latency_ms_quartiles,
    COUNT(DISTINCT k) AS distinct_ks,
    SIZE(COLLECT_SET(k)) AS num_ks
FROM base
GROUP BY DATE_TRUNC('DAY', log_ts)
