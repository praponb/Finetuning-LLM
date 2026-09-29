SELECT
    id,
    service,
    log_ts,
    status_code,
    payload_json,
    context,
    COALESCE(context['region'], 'direct') AS region_or_default,
    context['version'] AS version
FROM app_logs
WHERE service IN ('search', 'auth', 'billing');
