WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    id,
    log_ts,
    k,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'EXTENDED' WHEN k RLIKE '^(region)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS k_zone
FROM lines
ORDER BY id;
