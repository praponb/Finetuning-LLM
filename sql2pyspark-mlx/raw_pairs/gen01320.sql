select id, service, host, log_ts, level, latency_ms, payload_json, context, coalesce(context['version'], 'none') as version_or_default, cast((status_code & 4) != 0 as int) as has_fault from app_logs;
