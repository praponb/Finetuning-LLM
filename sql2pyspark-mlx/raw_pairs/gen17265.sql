select id, service, log_ts, context['version'] as version, shiftright(status_code, 2) as status_code_high, context['trace'] as trace from app_logs where to_date(log_ts) >= '2023-06-27'
