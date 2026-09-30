WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        attr_key,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE attr_key = 'version'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM parsed
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE latency_ms IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_code_high * 1.07, 2) AS status_code_high_scaled,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM reconciled
    WHERE attr_key = 'version'
)
-- Grouped metrics
SELECT
    attr_key,
    CASE WHEN flag_bit2 >= 1 THEN 'HIGH' ELSE 'LOW' END AS flag_bit2_tier,
    CASE WHEN status_code_high_scaled >= 61 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_scaled_tier,
    MAX(status_code_high) FILTER (WHERE attr_key = 'version') AS version_max_status_code_high,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    ROUND(SUM(status_code_high_scaled) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS status_code_high_scaled_per_trace,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM valid
GROUP BY attr_key, CASE WHEN flag_bit2 >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_high_scaled >= 61 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_attr_keys NULLS LAST;
