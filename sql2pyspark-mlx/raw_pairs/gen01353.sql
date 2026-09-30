WITH parsed AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        payload_json,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM app_logs
    WHERE host = 'h1'
)
SELECT
    host,
    PERCENTILE_APPROX(is_flagged, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_quartiles,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    BIT_AND(status_code) AS status_code_bit_and
FROM parsed
GROUP BY host
ORDER BY host DESC NULLS FIRST
