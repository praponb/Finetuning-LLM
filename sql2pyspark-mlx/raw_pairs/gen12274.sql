WITH lines AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
parsed AS (
    SELECT
        id,
        latency_ms,
        k,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
kept_keys AS (
    SELECT id
    FROM parsed
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE level IN ('ERROR', 'DEBUG')
),
kept_rows AS (
    SELECT s.*
    FROM parsed s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    SELECT
        latency_ms,
        is_flagged,
        PERCENT_RANK() OVER (ORDER BY status_code_high DESC) AS status_code_high_percent_rank,
        RANK() OVER (PARTITION BY k ORDER BY is_flagged) AS is_flagged_rank
    FROM kept_rows
)
SELECT
    latency_ms,
    is_flagged,
    status_code_high_percent_rank,
    is_flagged_rank
FROM enriched
ORDER BY latency_ms DESC;
