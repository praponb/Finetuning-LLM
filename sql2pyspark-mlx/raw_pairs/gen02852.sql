WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        latency_ms,
        attr_key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(is_flagged) OVER (PARTITION BY attr_key) AS attr_key_count_is_flagged,
        AVG(is_flagged) OVER (PARTITION BY attr_key) AS attr_key_avg_is_flagged
    FROM parsed
),
scored AS (
    -- Ranking and gap features
    SELECT *, COUNT(latency_ms) OVER w_running AS attr_key_count_latency_ms
    FROM with_windows
    WHERE attr_key != 'version'
    WINDOW w_running AS (PARTITION BY attr_key)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(attr_key_avg_is_flagged, 0) AS attr_key_avg_is_flagged_rounded,
        CASE WHEN attr_key_count_latency_ms >= 16 THEN 'High' WHEN attr_key_count_latency_ms >= 4 THEN 'Medium' ELSE 'Low' END AS attr_key_count_latency_ms_tier
    FROM scored
    WHERE attr_key IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN latency_ms >= 2074 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    ROUND(AVG(is_flagged), 2) AS avg_is_flagged,
    COUNT_IF(attr_key = 'trace') AS trace_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    COUNT_IF(attr_key = 'version') AS version_events,
    AVG(status_code_high) AS avg_status_code_high
FROM base
GROUP BY CASE WHEN latency_ms >= 2074 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT_IF(attr_key = 'trace') < 1000
ORDER BY version_events NULLS LAST;
