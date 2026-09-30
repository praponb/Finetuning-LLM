SELECT
    id,
    service,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    SIZE(context) AS num_context,
    COALESCE(context['trace'], 'none') AS trace_or_default,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs
WHERE level <> 'WARN';
