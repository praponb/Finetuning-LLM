WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT(*) FILTER (WHERE service = 'billing') AS billing_rows,
    BIT_OR(status_code) AS status_code_bit_or,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    CORR(status_code, latency_ms) AS corr_status_code_latency_ms
FROM flattened
GROUP BY DATE_TRUNC('DAY', log_ts)
ORDER BY activity_day NULLS FIRST;
