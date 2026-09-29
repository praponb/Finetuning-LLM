WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
metrics AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v,
        MAX(latency_ms) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_max_30m,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC) AS latency_ms_percent_rank
    FROM exploded
),
scored AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v,
        latency_ms_max_30m,
        latency_ms_percent_rank,
        AVG(latency_ms) OVER (PARTITION BY k, level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        MAX(latency_ms_max_30m) OVER (PARTITION BY k, service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_max_30m_max_1h,
        MAX(latency_ms_max_30m) OVER w_ordered AS latency_ms_max_30m_max_2h,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share
    FROM metrics
    WINDOW w_ordered AS (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v,
        latency_ms_max_30m,
        latency_ms_percent_rank,
        latency_ms_avg_15m,
        latency_ms_max_30m_max_1h,
        latency_ms_max_30m_max_2h,
        latency_ms_share,
        AVG(latency_ms) OVER (PARTITION BY k, service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_avg_30m,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share_2,
        STDDEV_SAMP(latency_ms_share) OVER (PARTITION BY level, host) AS level_stddev_samp_latency_ms_share,
        PERCENT_RANK() OVER (PARTITION BY service, k ORDER BY latency_ms_max_30m_max_2h DESC NULLS FIRST) AS latency_ms_max_30m_max_2h_percent_rank
    FROM scored
),
cleaned AS (
    SELECT
        service,
        log_ts,
        latency_ms,
        k,
        v,
        latency_ms_avg_15m,
        latency_ms_max_30m_max_1h,
        latency_ms_max_30m_max_2h,
        latency_ms_share,
        latency_ms_avg_30m,
        level_stddev_samp_latency_ms_share,
        latency_ms_max_30m_max_2h_percent_rank,
        COALESCE(latency_ms_share_2, 0) AS latency_ms_share_2_filled
    FROM enriched
    WHERE k IS NOT NULL
),
enriched_app_logs AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms_share DESC NULLS LAST) AS latency_ms_share_cume_dist,
        STDDEV_SAMP(latency_ms_max_30m_max_2h_percent_rank) OVER (PARTITION BY k) AS k_stddev_samp_latency_ms_max_30m_max_2h_percent_rank,
        RANK() OVER (ORDER BY latency_ms_share ASC) AS latency_ms_share_rank,
        MAX(latency_ms_avg_15m) OVER w_running AS service_max_latency_ms_avg_15m
    FROM cleaned
    WINDOW w_running AS (PARTITION BY service, k)
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM enriched_app_logs
GROUP BY 1;
