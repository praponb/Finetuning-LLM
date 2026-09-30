WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, MAX(latency_ms) OVER (PARTITION BY k) AS k_max_latency_ms
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(latency_ms, status_code) OVER w_running AS corr_latency_ms_status_code,
        CORR(status_code, latency_ms) OVER w_running AS corr_status_code_latency_ms
    FROM enriched
    WHERE latency_ms IS NOT NULL
    WINDOW w_running AS (PARTITION BY k)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(k, 1, 2) AS k_prefix
    FROM scored
    WHERE k IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        k_max_latency_ms,
        k_prefix,
        SUM(corr_status_code_latency_ms) OVER (PARTITION BY k) AS k_sum_corr_status_code_latency_ms
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    k,
    PERCENTILE_APPROX(latency_ms, 0.5) AS latency_ms_p50,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN k = 'trace' THEN 1 END), 0), 2) AS latency_ms_per_trace,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    PERCENTILE_APPROX(k_sum_corr_status_code_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS k_sum_corr_status_code_latency_ms_quartiles,
    SUM(CASE WHEN k = 'region' THEN status_code ELSE 0 END) AS region_status_code
FROM metrics
GROUP BY k
