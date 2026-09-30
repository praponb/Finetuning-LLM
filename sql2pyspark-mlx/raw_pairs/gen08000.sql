WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        v,
        LOWER(host) AS host_lower
    FROM exploded
    WHERE status_code >= 274
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(status_code * 1.1, 2) AS adjusted_status_code,
        status_code * latency_ms AS status_code_times_latency_ms
    FROM cleaned
    WHERE k IS NOT NULL
      AND status_code >= 332
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_hist AS adjusted_status_code_rank,
        CORR(latency_ms, status_code) OVER w_hist_2 AS corr_latency_ms_status_code,
        AVG(latency_ms) OVER w_hist_2 AS k_avg_latency_ms,
        CUME_DIST() OVER (PARTITION BY k ORDER BY adjusted_status_code) AS adjusted_status_code_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share
    FROM cleaned_app_logs
    WHERE adjusted_status_code <= 56
    WINDOW w_hist AS (ORDER BY adjusted_status_code), w_hist_2 AS (PARTITION BY k)
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN corr_latency_ms_status_code >= 0.51 THEN 'LARGE' ELSE 'SMALL' END AS corr_latency_ms_status_code_tier,
        k,
        SIZE(COLLECT_SET(k)) AS num_ks,
        ROUND(SUM(adjusted_status_code) / NULLIF(COUNT(CASE WHEN k = 'region' THEN 1 END), 0), 2) AS adjusted_status_code_per_region,
        ROUND(SUM(k_avg_latency_ms) / NULLIF(COUNT(CASE WHEN k = 'region' THEN 1 END), 0), 2) AS k_avg_latency_ms_per_region,
        COUNT(*) FILTER (WHERE k = 'trace') AS trace_rows,
        COUNT(*) FILTER (WHERE k = 'trace') AS trace_rows_2,
        AVG(k_avg_latency_ms) AS avg_k_avg_latency_ms
    FROM enriched
    GROUP BY CUBE (CASE WHEN corr_latency_ms_status_code >= 0.51 THEN 'LARGE' ELSE 'SMALL' END, k)
)
SELECT
    corr_latency_ms_status_code_tier,
    num_ks,
    adjusted_status_code_per_region,
    k_avg_latency_ms_per_region,
    trace_rows_2,
    avg_k_avg_latency_ms
FROM daily
ORDER BY avg_k_avg_latency_ms DESC, trace_rows_2 DESC
