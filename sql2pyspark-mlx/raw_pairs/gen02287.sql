SELECT
    id,
    service,
    level,
    latency_ms,
    payload_json,
    context,
    CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
    COALESCE(context['version'], 'unknown') AS version_or_default,
    SIZE(context) AS num_context
FROM app_logs;
