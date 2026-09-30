WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        payload_json,
        context,
        COALESCE(context['region'], 'unknown') AS region_or_default,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(DISTINCT level) AS distinct_levels,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high
FROM flattened_attrs
GROUP BY DATE_TRUNC('HOUR', log_ts);
