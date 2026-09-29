WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
unpacked AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        k,
        value,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
ranked AS (
    SELECT *
    FROM unpacked
    QUALIFY RANK() OVER (PARTITION BY k ORDER BY status_code_high DESC NULLS LAST) = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k, level ORDER BY log_ts, id, k) AS rn
        FROM ranked
    ) t
    WHERE rn = 1
)
SELECT
    level,
    CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    COUNT_IF(level = 'WARN') AS warn_events,
    APPROX_PERCENTILE(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles
FROM deduped
GROUP BY GROUPING SETS ((level, CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END), (level))
HAVING COUNT_IF(level = 'WARN') > 1
ORDER BY level DESC;
