WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
with_windows AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value,
        MAX(latency_ms) OVER w_ordered AS service_max_latency_ms,
        CUME_DIST() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_cume_dist
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY service)
),
cleaned AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value,
        service_max_latency_ms,
        CONCAT_WS('-', attr_key, service) AS attr_key_service_key,
        CASE WHEN latency_ms_cume_dist >= 0.89 THEN 'High' WHEN latency_ms_cume_dist >= 0.24 THEN 'Medium' ELSE 'Low' END AS latency_ms_cume_dist_level,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM with_windows
    WHERE attr_key IN ('region', 'version')
),
scored AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY service_max_latency_ms DESC NULLS LAST) AS service_max_latency_ms_rank,
        STDDEV_SAMP(service_max_latency_ms) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_service_max_latency_ms,
        SUM(service_max_latency_ms) OVER w_recent AS level_sum_service_max_latency_ms,
        RANK() OVER (PARTITION BY level ORDER BY service_max_latency_ms NULLS LAST) AS service_max_latency_ms_rank_2,
        ROUND(service_max_latency_ms / SUM(service_max_latency_ms) OVER (PARTITION BY attr_key), 4) AS service_max_latency_ms_share
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY level)
),
valid AS (
    SELECT
        *,
        COALESCE(service_max_latency_ms_share, 0) AS service_max_latency_ms_share_filled,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key_2
    FROM scored
    WHERE attr_key IS NOT NULL
      AND log_ts >= '2023-05-14'
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY service_max_latency_ms NULLS FIRST) AS service_max_latency_ms_rank_3,
        MAX(service_max_latency_ms_share_filled) OVER (PARTITION BY attr_key) AS attr_key_max_service_max_latency_ms_share_filled,
        SUM(service_max_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS service_max_latency_ms_sum_2h
    FROM valid
)
SELECT
    id,
    log_ts,
    level,
    latency_ms,
    attr_key,
    service_max_latency_ms,
    attr_key_service_key,
    latency_ms_cume_dist_level,
    resolved_attr_key
FROM enriched
