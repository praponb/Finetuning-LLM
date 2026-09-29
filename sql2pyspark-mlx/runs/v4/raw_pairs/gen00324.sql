SELECT
    id,
    host,
    log_ts,
    status_code,
    context,
    CAST((status_code & 1) != 0 AS INT) AS has_fault,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    SIZE(context) AS num_context
FROM app_logs
