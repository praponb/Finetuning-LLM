WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
)
SELECT
    id,
    level,
    k,
    value,
    CASE WHEN level RLIKE '^(DEBUG|INFO)$' THEN 'PRIORITY' ELSE 'CORE' END AS level_zone
FROM exploded
ORDER BY id ASC;
