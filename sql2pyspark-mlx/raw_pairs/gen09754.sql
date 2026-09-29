SELECT id, service, status_code, context['region'] AS region, COALESCE(context['trace'], 'direct') AS trace_or_default FROM app_logs
