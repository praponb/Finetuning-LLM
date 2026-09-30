SELECT
    id,
    host,
    level,
    latency_ms,
    status_code,
    payload_json,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    context['trace'] AS trace
FROM app_logs
