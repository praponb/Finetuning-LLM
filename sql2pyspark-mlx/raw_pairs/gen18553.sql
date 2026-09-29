SELECT
    id,
    service,
    log_ts,
    status_code,
    context,
    context['region'] AS region,
    SHIFTRIGHT(status_code, 2) AS status_code_high
FROM app_logs
WHERE service = 'checkout';
