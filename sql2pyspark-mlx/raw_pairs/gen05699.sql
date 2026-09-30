WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
)
SELECT
    id,
    service,
    host,
    k,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS k_zone
FROM lines
ORDER BY host DESC;
