WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
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
        k,
        v,
        CASE WHEN host IN ('h2', 'h4') THEN 'Core' ELSE 'Other' END AS host_group,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM lines
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'version'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        k,
        v,
        host_group,
        resolved_k,
        CASE WHEN latency_ms >= 2115 THEN 'High' WHEN latency_ms >= 872 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        COALESCE(k, 'N/A') AS k_clean
    FROM cleaned
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        k,
        host_group,
        SUM(latency_ms) OVER w_running AS k_sum_latency_ms,
        CUME_DIST() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist
    FROM base
    WINDOW w_running AS (PARTITION BY k)
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        k,
        k_sum_latency_ms,
        latency_ms_cume_dist,
        CASE WHEN k IN ('version', 'region') THEN 'Group A' ELSE 'Other' END AS k_group,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM metrics
    WHERE k IS NOT NULL
),
metrics_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        k,
        k_group,
        k_clean,
        CUME_DIST() OVER (PARTITION BY k ORDER BY k_sum_latency_ms NULLS FIRST) AS k_sum_latency_ms_cume_dist
    FROM base_app_logs
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        k_group,
        k_clean,
        STDDEV_SAMP(k_sum_latency_ms_cume_dist) OVER (PARTITION BY k) AS k_stddev_samp_k_sum_latency_ms_cume_dist
    FROM metrics_app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN k_stddev_samp_k_sum_latency_ms_cume_dist >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS k_stddev_samp_k_sum_latency_ms_cume_dist_tier,
    PERCENTILE_APPROX(k_stddev_samp_k_sum_latency_ms_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS k_stddev_samp_k_sum_latency_ms_cume_dist_quartiles,
    PERCENTILE_APPROX(k_stddev_samp_k_sum_latency_ms_cume_dist, 0.9) AS k_stddev_samp_k_sum_latency_ms_cume_dist_p90,
    APPROX_PERCENTILE(k_stddev_samp_k_sum_latency_ms_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS k_stddev_samp_k_sum_latency_ms_cume_dist_quartiles_2,
    COUNT(*) AS num_rows,
    ROUND(AVG(k_stddev_samp_k_sum_latency_ms_cume_dist), 2) AS avg_k_stddev_samp_k_sum_latency_ms_cume_dist,
    ROUND(AVG(k_stddev_samp_k_sum_latency_ms_cume_dist), 2) AS avg_k_stddev_samp_k_sum_latency_ms_cume_dist_2
FROM enriched
GROUP BY CASE WHEN k_stddev_samp_k_sum_latency_ms_cume_dist >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END;
