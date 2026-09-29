SELECT
    id,
    service,
    host,
    log_ts,
    COALESCE(context['region'], 'none') AS region_or_default,
    COALESCE(context['trace'], 'direct') AS trace_or_default
FROM app_logs
WHERE host IN ('h2', 'h4', 'h1')
