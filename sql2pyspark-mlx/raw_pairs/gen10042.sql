WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        value,
        CORR(latency_ms, status_code) OVER (PARTITION BY service, level) AS corr_latency_ms_status_code,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share,
        AVG(status_code) OVER (PARTITION BY service, attr_key) AS service_avg_status_code,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_avg_1h
    FROM exploded
    WHERE level = 'INFO'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        status_code,
        attr_key,
        value,
        corr_latency_ms_status_code,
        latency_ms_share,
        service_avg_status_code,
        status_code_avg_1h,
        CAST(status_code_avg_1h AS STRING) AS status_code_avg_1h_num,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM enriched
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 11:00:00')
      AND attr_key IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        attr_key,
        corr_latency_ms_status_code,
        attr_key_clean,
        ROUND(status_code_avg_1h / SUM(status_code_avg_1h) OVER (PARTITION BY service, attr_key), 4) AS status_code_avg_1h_share,
        ROUND(service_avg_status_code / SUM(service_avg_status_code) OVER (PARTITION BY service), 4) AS service_avg_status_code_share,
        MIN(latency_ms_share) OVER (PARTITION BY service) AS service_min_latency_ms_share,
        ROUND(status_code_avg_1h / SUM(status_code_avg_1h) OVER (PARTITION BY attr_key), 4) AS status_code_avg_1h_share_2
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN service_avg_status_code_share >= 0.63 THEN 'HIGH' ELSE 'LOW' END AS service_avg_status_code_share_tier,
    SIZE(COLLECT_SET(service)) AS num_services,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(status_code_avg_1h_share_2, 0.5) AS status_code_avg_1h_share_2_p50,
    APPROX_PERCENTILE(service_min_latency_ms_share, ARRAY(0.25, 0.5, 0.75)) AS service_min_latency_ms_share_quartiles,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE attr_key = 'region') AS region_rows
FROM with_windows
GROUP BY CASE WHEN service_avg_status_code_share >= 0.63 THEN 'HIGH' ELSE 'LOW' END
HAVING region_rows >= 1
ORDER BY num_services DESC NULLS LAST
