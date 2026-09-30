WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        context['region'] AS region,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['version'], 'none') AS version_or_default,
        context['trace'] AS trace
    FROM app_logs
)
-- Grouped metrics
SELECT
    host,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count,
    SUM(flag_bit1) AS total_flag_bit1,
    MAX(log_ts) AS last_log_ts,
    APPROX_PERCENTILE(flag_bit1, 0.95) AS flag_bit1_p95,
    COUNT_IF(host = 'h3') AS h3_events,
    GROUPING_ID() AS gid
FROM flattened_attrs
GROUP BY CUBE (host)
ORDER BY h3_events
