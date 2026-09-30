WITH lines AS (
    SELECT
        id,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    id,
    level,
    latency_ms,
    k
FROM lines
