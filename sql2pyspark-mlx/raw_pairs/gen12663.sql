WITH parsed AS (
    SELECT
        id,
        service,
        status_code,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
    WHERE service IN ('auth', 'billing', 'checkout')
)
SELECT
    id,
    status_code,
    duration_text,
    COUNT(status_code) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_status_code,
    SUM(status_code) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_status_code,
    AVG(status_code) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_code,
    MAX(status_code) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_status_code,
    LEAD(status_code, 2, 0) OVER (PARTITION BY service ORDER BY status_code_high, id) AS next_status_code
FROM parsed
