WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        COALESCE(context['version'], 'direct') AS version_or_default,
        context['version'] AS version,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace
    FROM app_logs
)
SELECT
    id,
    service,
    host,
    log_ts,
    level,
    context,
    user_id,
    version_or_default,
    version,
    has_trace,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts, id) AS seq_no
FROM unpacked;
