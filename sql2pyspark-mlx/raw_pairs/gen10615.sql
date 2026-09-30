WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
)
SELECT
    id,
    service,
    host,
    log_ts,
    key,
    CASE WHEN host RLIKE '^(h3|h2)$' THEN 'GROUP_A' WHEN host RLIKE '^(h1|h4)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS host_zone
FROM lines
ORDER BY service DESC NULLS FIRST;
