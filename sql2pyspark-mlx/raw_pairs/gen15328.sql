SELECT
    id,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    CAST((status_code & 4) != 0 AS INT) AS has_fault
FROM app_logs;
