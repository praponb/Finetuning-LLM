SELECT
    id,
    host,
    log_ts,
    latency_ms,
    status_code,
    payload_json,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    context['version'] AS version,
    COALESCE(context['trace'], 'none') AS trace_or_default
FROM app_logs
