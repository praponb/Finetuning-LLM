WITH parsed AS (
    SELECT
        id,
        service,
        host,
        level,
        payload_json,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
)
SELECT
    CASE WHEN retry_count >= 4 THEN 'LARGE' ELSE 'SMALL' END AS retry_count_tier,
    SUM(CASE WHEN service = 'checkout' THEN retry_count ELSE 0 END) AS checkout_retry_count,
    SUM(CASE WHEN host = 'h3' THEN retry_count ELSE 0 END) AS h3_retry_count,
    ROUND(AVG(retry_count), 2) AS avg_retry_count,
    MAX(retry_count) FILTER (WHERE service = 'billing') AS billing_max_retry_count
FROM parsed
GROUP BY CUBE (CASE WHEN retry_count >= 4 THEN 'LARGE' ELSE 'SMALL' END);
