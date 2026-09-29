WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        AVG(latency_ms) OVER (PARTITION BY k) k_avg_latency_ms
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) status_code_share,
        ROUND(k_avg_latency_ms / SUM(k_avg_latency_ms) OVER (PARTITION BY host), 4) k_avg_latency_ms_share,
        MIN(k_avg_latency_ms) OVER (PARTITION BY host) host_min_k_avg_latency_ms,
        MAX(k_avg_latency_ms) OVER (PARTITION BY host, service) host_max_k_avg_latency_ms,
        RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS LAST) latency_ms_rank
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_ordered k_avg_latency_ms_share_dense_rank,
        MIN(host_min_k_avg_latency_ms) OVER w_hist service_min_host_min_k_avg_latency_ms,
        AVG(k_avg_latency_ms_share) OVER (PARTITION BY host, service) host_avg_k_avg_latency_ms_share,
        PERCENT_RANK() OVER (ORDER BY latency_ms NULLS FIRST) latency_ms_percent_rank,
        SUM(host_max_k_avg_latency_ms) OVER w_hist service_sum_host_max_k_avg_latency_ms
    FROM metrics
    WHERE host_min_k_avg_latency_ms <= 1153
    WINDOW w_ordered AS (PARTITION BY service, host ORDER BY k_avg_latency_ms_share NULLS LAST), w_hist AS (PARTITION BY service)
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(status_code, latency_ms) OVER (PARTITION BY service) corr_status_code_latency_ms,
        ROUND(host_max_k_avg_latency_ms / SUM(host_max_k_avg_latency_ms) OVER (PARTITION BY service, host), 4) host_max_k_avg_latency_ms_share,
        CUME_DIST() OVER (ORDER BY k_avg_latency_ms_share DESC) k_avg_latency_ms_share_cume_dist
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN host_max_k_avg_latency_ms_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END AS host_max_k_avg_latency_ms_share_tier,
    APPROX_PERCENTILE(host_min_k_avg_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS host_min_k_avg_latency_ms_quartiles,
    APPROX_PERCENTILE(host_min_k_avg_latency_ms, 0.9) AS host_min_k_avg_latency_ms_p90,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    ROUND(SUM(host_min_k_avg_latency_ms) / NULLIF(COUNT(CASE WHEN host = 'h4' THEN 1 END), 0), 2) AS host_min_k_avg_latency_ms_per_h4
FROM metrics_app_logs
GROUP BY CASE WHEN host_max_k_avg_latency_ms_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END
ORDER BY host_max_k_avg_latency_ms_share_tier DESC NULLS FIRST
