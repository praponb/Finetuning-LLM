WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
),
base AS (
    SELECT
        id,
        service,
        latency_ms,
        k,
        value,
        CAST(log_ts AS DATE) AS log_ts_day,
        CASE WHEN k = 'trace' THEN 1 ELSE 0 END AS is_trace
    FROM lines
    WHERE log_ts >= DATE '2025-06-01'
),
with_windows AS (
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        AVG(is_trace) OVER (PARTITION BY service ORDER BY latency_ms, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS is_trace_avg_6,
        MAX(latency_ms) OVER (PARTITION BY MONTH(log_ts_day), k) AS log_ts_day_month_max_latency_ms
    FROM base
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms_share DESC, id, k) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY is_trace, id, k) = 1
),
ranked_app_logs AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS FIRST, id, k) AS row_num_2
        FROM deduped
    ) ranked
    WHERE row_num_2 = 1
)
SELECT
    CASE WHEN is_trace_avg_6 >= 78 THEN 'LARGE' ELSE 'SMALL' END AS is_trace_avg_6_tier,
    DATE_TRUNC('WEEK', log_ts_day) AS week_start,
    DATE_TRUNC('QUARTER', log_ts_day) AS quarter_start,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN k = 'trace' THEN log_ts_day_month_max_latency_ms ELSE 0 END) AS trace_log_ts_day_month_max_latency_ms,
    ROUND(AVG(is_trace), 2) AS avg_is_trace,
    COUNT_IF(k = 'version') AS version_events
FROM ranked_app_logs
GROUP BY CASE WHEN is_trace_avg_6 >= 78 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', log_ts_day), DATE_TRUNC('QUARTER', log_ts_day)
ORDER BY version_events DESC
