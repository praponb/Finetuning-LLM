SELECT
    id,
    service,
    latency_ms,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    COALESCE(context['region'], 'unknown') AS region_or_default,
    context['version'] AS version,
    context['region'] AS region,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
FROM app_logs
