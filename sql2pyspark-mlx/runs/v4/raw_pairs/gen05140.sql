SELECT id, service, log_ts, latency_ms, status_code, context, COALESCE(context['trace'], 'direct') AS trace_or_default, COALESCE(context['version'], 'none') AS version_or_default FROM app_logs
