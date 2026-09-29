SELECT
    id,
    service,
    level,
    latency_ms,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    COALESCE(context['trace'], 'unknown') AS trace_or_default,
    COALESCE(context['version'], 'none') AS version_or_default
FROM app_logs
WHERE service = 'search'
