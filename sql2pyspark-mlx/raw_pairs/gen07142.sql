SELECT
    id,
    service,
    host,
    payload_json,
    context,
    COALESCE(context['region'], 'unknown') AS region_or_default,
    ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    SIZE(context) AS num_context
FROM app_logs;
