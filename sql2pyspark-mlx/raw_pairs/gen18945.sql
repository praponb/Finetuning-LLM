WITH flattened_attrs AS (
    SELECT
        id,
        host,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        COALESCE(context['region'], 'unknown') AS region_or_default,
        SIZE(context) AS num_context
    FROM app_logs
    WHERE host = 'h3'
)
SELECT
    id,
    duration_text,
    MIN(num_context) OVER w_grp AS last3_min_num_context,
    MAX(num_context) OVER w_grp AS last3_max_num_context,
    SUM(num_context) OVER w_grp AS last3_sum_num_context
FROM flattened_attrs
WHERE num_context <= 67
WINDOW w_grp AS (PARTITION BY host ORDER BY num_context, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
