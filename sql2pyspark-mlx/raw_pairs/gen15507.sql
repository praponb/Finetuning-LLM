WITH flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
SELECT
    id,
    host,
    status_code,
    has_trace,
    region,
    duration_sec,
    tier,
    NTILE(10) OVER (ORDER BY status_code DESC, id) status_code_bucket,
    SUM(duration_sec) OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
    AVG(status_code) OVER w_hist status_code_avg_5
FROM flattened_attrs
WINDOW w_hist AS (PARTITION BY service, level ORDER BY status_code, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW);
