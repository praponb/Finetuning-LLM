SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    COALESCE(context['region'], 'direct') AS region_or_default
FROM app_logs
WHERE latency_ms IS NULL
