SELECT id, log_ts, latency_ms, payload_json, GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id, SIZE(context) AS num_context FROM app_logs WHERE latency_ms >= 1699;
