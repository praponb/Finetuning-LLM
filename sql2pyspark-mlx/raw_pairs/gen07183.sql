SELECT id, service, log_ts, GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier, COALESCE(context['version'], 'unknown') AS version_or_default FROM app_logs WHERE service = 'checkout';
