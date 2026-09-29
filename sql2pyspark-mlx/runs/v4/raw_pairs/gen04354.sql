WITH exploded AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    key,
    SUM(CASE WHEN key = 'region' THEN status_code ELSE 0 END) AS region_status_code,
    CORR(latency_ms, status_code) AS corr_latency_ms_status_code,
    AVG(status_code) FILTER (WHERE key = 'version') AS version_avg_status_code,
    SUM(CASE WHEN key = 'region' THEN latency_ms ELSE 0 END) AS region_latency_ms,
    GROUPING_ID() AS gid
FROM exploded
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', log_ts), key), (DATE_TRUNC('HOUR', log_ts)), ())
