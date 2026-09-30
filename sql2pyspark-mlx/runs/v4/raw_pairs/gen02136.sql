select id, latency_ms, context['region'] as region, get_json_object(payload_json, '$.user.tier') as user_tier from app_logs
