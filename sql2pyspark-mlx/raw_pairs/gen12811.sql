SELECT
    id,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
    context['trace'] AS trace,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    SIZE(context) AS num_context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs
