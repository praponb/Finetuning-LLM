SELECT id, host, log_ts, payload_json, context['trace'] AS trace, COALESCE(context['trace'], 'unknown') AS trace_or_default FROM app_logs
