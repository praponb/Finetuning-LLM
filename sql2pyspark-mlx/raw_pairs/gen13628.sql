WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
)
SELECT
    id,
    log_ts,
    latency_ms,
    attr_key,
    v,
    CASE WHEN attr_key RLIKE '^(version|trace)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(region)$' THEN 'CORE' ELSE 'EXTENDED' END AS attr_key_zone
FROM lines
