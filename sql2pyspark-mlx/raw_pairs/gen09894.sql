WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
)
SELECT
    id,
    log_ts,
    attr_key,
    value,
    CASE WHEN attr_key RLIKE '^(region|trace)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(version)$' THEN 'CORE' ELSE 'EXTENDED' END AS attr_key_zone
FROM lines
ORDER BY attr_key DESC, attr_key_zone ASC;
