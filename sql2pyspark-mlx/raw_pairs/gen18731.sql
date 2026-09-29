WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN k IN ('trace', 'version') THEN 'Group A' ELSE 'Other' END AS k_group,
        CASE WHEN host IN ('h4', 'h1') THEN 'Priority' ELSE 'Other' END AS host_group
    FROM flattened
    WHERE host = 'h2'
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        k,
        k_group,
        host_group,
        SUBSTRING(host, 1, 3) AS host_prefix,
        CAST(log_ts AS DATE) AS log_ts_day
    FROM base
    WHERE log_ts >= TIMESTAMP '2025-06-01 08:00:00'
      AND host != 'h1'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY k, host ORDER BY log_ts, id, k) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), host ORDER BY log_ts_day DESC, id, k) AS event_seq_2
    FROM base_app_logs
),
base_app_logs_2 AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', host, k) AS host_k_key
    FROM enriched
    WHERE log_ts > DATE '2025-06-01'
      AND k IS DISTINCT FROM 'version'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS event_seq_per_h3,
    AVG(event_seq_2) FILTER (WHERE host = 'h4') AS h4_avg_event_seq_2,
    SUM(CASE WHEN k = 'trace' THEN event_seq ELSE 0 END) AS trace_event_seq,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count,
    COUNT_IF(k = 'trace') AS trace_events
FROM base_app_logs_2
GROUP BY DATE_TRUNC('DAY', log_ts)
