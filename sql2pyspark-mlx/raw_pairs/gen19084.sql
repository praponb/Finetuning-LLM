SELECT
    id,
    service,
    latency_ms,
    status_code,
    context,
    COALESCE(context['trace'], 'unknown') AS trace_or_default,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
FROM app_logs
WHERE latency_ms <= 2216
