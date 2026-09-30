WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        status_code_high,
        INITCAP(TRIM(attr_key)) attr_key_title
    FROM parsed
    WHERE log_ts > TIMESTAMP '2025-06-01 07:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        attr_key,
        LAG(latency_ms) OVER w_recent prev_latency_ms,
        LEAD(latency_ms) OVER w_recent next_latency_ms,
        LEAD(status_code_high) OVER w_recent next_status_code_high,
        ROW_NUMBER() OVER w_recent seq_no,
        SUM(latency_ms) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) latency_ms_sum_30m
    FROM valid
    WINDOW w_recent AS (PARTITION BY attr_key ORDER BY status_code_high, id, attr_key)
)
-- Multi-level aggregation
SELECT
    seq_no,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    SUM(CASE WHEN attr_key = 'trace' THEN latency_ms_sum_30m ELSE 0 END) AS trace_latency_ms_sum_30m,
    ROUND(SUM(latency_ms_sum_30m) / NULLIF(COUNT(CASE WHEN attr_key = 'version' THEN 1 END), 0), 2) AS latency_ms_sum_30m_per_version
FROM with_windows
GROUP BY seq_no
ORDER BY trace_latency_ms_sum_30m DESC
