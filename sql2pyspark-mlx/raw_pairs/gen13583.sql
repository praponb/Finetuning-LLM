WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        k,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM parsed
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE status_code <= 349
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_flagged AS DOUBLE) AS is_flagged_num,
        SUBSTRING(host, 1, 4) AS host_prefix
    FROM reconciled
    WHERE k IS NOT NULL
)
SELECT
    id,
    host,
    attr_value,
    status_code_high,
    is_flagged_num,
    host_prefix,
    CASE WHEN host RLIKE '^(h1|h3)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS host_zone
FROM valid
ORDER BY id;
