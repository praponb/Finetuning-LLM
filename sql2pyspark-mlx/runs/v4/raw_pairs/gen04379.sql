WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        status_code,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
SELECT
    CASE WHEN status_code >= 451 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    host,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    SORT_ARRAY(COLLECT_SET(tier)) AS tier_list,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN service = 'search' THEN 1 END), 0), 2) AS status_code_per_search,
    SIZE(COLLECT_SET(service)) AS num_services,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code >= 451 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(service)) DESC) AS num_services_dense_rank
FROM unpacked
GROUP BY 1, 2
HAVING num_services < 1
