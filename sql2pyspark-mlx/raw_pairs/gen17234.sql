WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
)
SELECT
    service,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(service)) AS num_services,
    MAX(log_ts) AS last_seen
FROM flattened
GROUP BY service
ORDER BY service DESC NULLS FIRST;
