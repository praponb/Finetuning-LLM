WITH unpacked AS (
    SELECT
        id,
        service,
        status_code,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
SELECT
    service,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    BIT_OR(status_code) AS status_code_bit_or,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN service = 'auth' THEN 1 END) AS auth_count,
    COUNT(*) AS cnt
FROM unpacked
GROUP BY service
HAVING COUNT(*) >= 1
ORDER BY status_code_bit_or DESC NULLS LAST
