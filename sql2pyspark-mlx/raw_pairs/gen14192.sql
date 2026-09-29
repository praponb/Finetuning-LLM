SELECT
    id,
    service,
    level,
    payload_json,
    context['region'] AS region,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
FROM app_logs
