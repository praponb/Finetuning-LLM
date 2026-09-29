WITH lines AS (
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
base AS (
    SELECT
        id,
        host,
        attr_key,
        v,
        CASE WHEN host IN ('h4', 'h3') THEN 'Priority' ELSE 'Other' END AS host_group,
        ROUND(latency_ms * 100, 2) AS latency_ms_scaled
    FROM lines
    WHERE attr_key IS NOT NULL
),
base_app_logs AS (
    SELECT
        id,
        host,
        v,
        host_group,
        latency_ms_scaled,
        UPPER(attr_key) AS attr_key_upper,
        UPPER(host) AS host_upper
    FROM base
    WHERE attr_key IS DISTINCT FROM 'region'
      AND latency_ms_scaled >= 71
),
with_windows AS (
    SELECT
        id,
        attr_key_upper,
        host_upper,
        MIN(latency_ms_scaled) OVER (PARTITION BY host) AS host_min_latency_ms_scaled,
        STDDEV_SAMP(latency_ms_scaled) OVER (PARTITION BY host) AS host_stddev_samp_latency_ms_scaled,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY latency_ms_scaled DESC NULLS LAST) AS latency_ms_scaled_dense_rank
    FROM base_app_logs
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY host_min_latency_ms_scaled DESC NULLS LAST) AS host_min_latency_ms_scaled_rank,
        CUME_DIST() OVER (ORDER BY host_stddev_samp_latency_ms_scaled DESC NULLS FIRST) AS host_stddev_samp_latency_ms_scaled_cume_dist,
        CUME_DIST() OVER (ORDER BY host_stddev_samp_latency_ms_scaled) AS host_stddev_samp_latency_ms_scaled_cume_dist_2
    FROM with_windows
),
enriched AS (
    SELECT
        attr_key_upper,
        latency_ms_scaled_dense_rank,
        host_stddev_samp_latency_ms_scaled_cume_dist,
        host_stddev_samp_latency_ms_scaled_cume_dist_2,
        DENSE_RANK() OVER w_running AS host_stddev_samp_latency_ms_scaled_cume_dist_2_dense_rank,
        RANK() OVER (ORDER BY host_stddev_samp_latency_ms_scaled DESC NULLS LAST) AS host_stddev_samp_latency_ms_scaled_rank,
        DENSE_RANK() OVER w_hist AS host_stddev_samp_latency_ms_scaled_dense_rank,
        RANK() OVER w_running AS host_stddev_samp_latency_ms_scaled_cume_dist_2_rank
    FROM metrics
    WINDOW w_running AS (ORDER BY host_stddev_samp_latency_ms_scaled_cume_dist_2 DESC NULLS FIRST), w_hist AS (ORDER BY host_stddev_samp_latency_ms_scaled)
)
SELECT
    host_stddev_samp_latency_ms_scaled_cume_dist_2_dense_rank,
    host_stddev_samp_latency_ms_scaled_rank,
    host_stddev_samp_latency_ms_scaled_cume_dist_2_rank,
    CUME_DIST() OVER w_hist AS host_stddev_samp_latency_ms_scaled_dense_rank_cume_dist,
    RANK() OVER (ORDER BY host_stddev_samp_latency_ms_scaled_cume_dist) AS host_stddev_samp_latency_ms_scaled_cume_dist_rank,
    RANK() OVER (ORDER BY host_stddev_samp_latency_ms_scaled_cume_dist_2_rank DESC NULLS LAST) AS host_stddev_samp_latency_ms_scaled_cume_dist_2_rank_rank
FROM enriched
WINDOW w_hist AS (ORDER BY host_stddev_samp_latency_ms_scaled_dense_rank DESC NULLS FIRST);
