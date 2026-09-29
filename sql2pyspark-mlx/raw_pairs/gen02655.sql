SELECT
    id,
    service,
    host,
    log_ts,
    latency_ms,
    payload_json,
    context['trace'] AS trace,
    ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
    CAST((status_code & 8) != 0 AS INT) AS is_flagged,
    SHIFTRIGHT(status_code, 2) AS status_code_high
FROM app_logs;
