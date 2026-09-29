WITH flattened AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    id,
    host,
    status_code,
    k,
    v
FROM flattened
ORDER BY k DESC NULLS LAST;
