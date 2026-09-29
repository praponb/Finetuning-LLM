WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    context,
    duration_text,
    duration_sec,
    NTILE(10) OVER (PARTITION BY service ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
    LAG(duration_sec, 2, 0) OVER (PARTITION BY service ORDER BY duration_sec, id) AS prev_duration_sec,
    NTILE(4) OVER (PARTITION BY service ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2
FROM unpacked
