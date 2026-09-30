SELECT
    id,
    service,
    host,
    level,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    COALESCE(context['version'], 'none') AS version_or_default,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    context['version'] AS version
FROM app_logs
WHERE service IN ('search', 'billing');
