WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(status_code_high) OVER (PARTITION BY k ORDER BY has_fault, id, k) AS next_status_code_high,
        LEAD(status_code) OVER (PARTITION BY k ORDER BY has_fault, id, k) AS next_status_code,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY has_fault, id, k) AS row_seq,
        STDDEV_SAMP(status_code) OVER (PARTITION BY k) AS k_stddev_samp_status_code
    FROM parsed
    WHERE k IN ('version', 'region')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code, id, k) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status_code,
        k,
        has_fault,
        next_status_code_high,
        next_status_code,
        row_seq,
        rank_in_group,
        COALESCE(k_stddev_samp_status_code, 0) AS k_stddev_samp_status_code_filled,
        COALESCE(k, 'NONE') AS k_filled
    FROM latest
    WHERE k IS DISTINCT FROM 'trace'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        next_status_code + next_status_code_high AS next_status_code_plus_next_status_code_high,
        has_fault * next_status_code_high AS has_fault_times_next_status_code_high,
        COALESCE(k, 'N/A') AS resolved_k
    FROM valid
    WHERE k IS NOT NULL
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN status_code >= 396 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
        ROUND(AVG(next_status_code_high), 2) AS avg_next_status_code_high,
        COUNT(CASE WHEN k = 'trace' THEN 1 END) AS trace_count
    FROM filtered
    GROUP BY CASE WHEN status_code >= 396 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    status_code_tier,
    version_count,
    avg_next_status_code_high,
    trace_count
FROM grouped
ORDER BY status_code_tier
