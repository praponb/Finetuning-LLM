WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        host,
        level,
        latency_ms,
        value,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
remaining_keys AS (
    -- Set difference on keys
    SELECT level
    FROM unpacked
    EXCEPT
    SELECT level
    FROM app_logs
    WHERE level IN ('DEBUG', 'ERROR', 'WARN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM unpacked s
    JOIN remaining_keys k ON s.level = k.level
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code_high NULLS FIRST) status_code_high_percent_rank,
        MAX(latency_ms) OVER (PARTITION BY host) host_max_latency_ms,
        COUNT(has_fault) OVER (PARTITION BY host, level) host_count_has_fault,
        MIN(latency_ms) OVER (PARTITION BY level) level_min_latency_ms,
        AVG(status_code_high) OVER (PARTITION BY level, host) level_avg_status_code_high
    FROM reconciled
)
SELECT
    host,
    level,
    latency_ms,
    value,
    has_fault,
    status_code_high,
    status_code_high_percent_rank,
    host_max_latency_ms,
    host_count_has_fault,
    CASE WHEN host RLIKE '^(h1|h2)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS host_zone
FROM metrics
ORDER BY host_count_has_fault DESC;
