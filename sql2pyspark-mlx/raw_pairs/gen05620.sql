WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
with_windows AS (
    SELECT
        host,
        status_code,
        attr_key,
        CORR(latency_ms, status_code) OVER (PARTITION BY host, level) corr_latency_ms_status_code
    FROM exploded
),
metrics AS (
    SELECT
        *,
        SUM(corr_latency_ms_status_code) OVER w_grp attr_key_sum_corr_latency_ms_status_code,
        RANK() OVER (PARTITION BY attr_key, host ORDER BY status_code ASC) status_code_rank,
        CUME_DIST() OVER w_recent corr_latency_ms_status_code_cume_dist
    FROM with_windows
    WHERE attr_key = 'region'
    WINDOW w_grp AS (PARTITION BY attr_key), w_recent AS (PARTITION BY host ORDER BY corr_latency_ms_status_code DESC)
),
metrics_app_logs AS (
    SELECT
        *,
        ROUND(status_code_rank / SUM(status_code_rank) OVER (PARTITION BY attr_key), 4) status_code_rank_share,
        CUME_DIST() OVER (ORDER BY corr_latency_ms_status_code DESC NULLS LAST) corr_latency_ms_status_code_cume_dist_2,
        RANK() OVER (ORDER BY corr_latency_ms_status_code DESC) corr_latency_ms_status_code_rank,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY corr_latency_ms_status_code_cume_dist DESC) corr_latency_ms_status_code_cume_dist_percent_rank
    FROM metrics
),
metrics_app_logs_2 AS (
    SELECT
        host,
        corr_latency_ms_status_code,
        attr_key_sum_corr_latency_ms_status_code,
        status_code_rank,
        corr_latency_ms_status_code_cume_dist,
        corr_latency_ms_status_code_rank,
        AVG(attr_key_sum_corr_latency_ms_status_code) OVER w_ordered host_avg_attr_key_sum_corr_latency_ms_status_code,
        STDDEV_SAMP(corr_latency_ms_status_code_cume_dist_2) OVER w_grp host_stddev_samp_corr_latency_ms_status_code_cume_dist_2,
        AVG(status_code_rank_share) OVER w_grp host_avg_status_code_rank_share,
        AVG(corr_latency_ms_status_code_cume_dist_percent_rank) OVER w_grp host_avg_corr_latency_ms_status_code_cume_dist_percent_rank,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code DESC) status_code_cume_dist
    FROM metrics_app_logs
    WINDOW w_ordered AS (PARTITION BY host, attr_key), w_grp AS (PARTITION BY host)
)
SELECT
    corr_latency_ms_status_code,
    attr_key_sum_corr_latency_ms_status_code,
    status_code_rank,
    corr_latency_ms_status_code_cume_dist,
    corr_latency_ms_status_code_rank,
    host_avg_attr_key_sum_corr_latency_ms_status_code,
    host_stddev_samp_corr_latency_ms_status_code_cume_dist_2,
    status_code_cume_dist,
    SUM(host_stddev_samp_corr_latency_ms_status_code_cume_dist_2) OVER (PARTITION BY host) host_sum_host_stddev_samp_corr_latency_ms_status_code_cume_dist_2,
    PERCENT_RANK() OVER w_part status_code_cume_dist_percent_rank
FROM metrics_app_logs_2
WINDOW w_part AS (ORDER BY status_code_cume_dist DESC NULLS FIRST);
