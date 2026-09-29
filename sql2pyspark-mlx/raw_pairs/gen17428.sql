WITH flattened_attrs AS (
    SELECT
        id,
        service,
        level,
        status_code,
        context,
        COALESCE(context['region'], 'none') AS region_or_default,
        context['region'] AS region,
        COALESCE(context['version'], 'direct') AS version_or_default,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
SELECT
    CASE WHEN status_code >= 411 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    BIT_OR(status_code) AS status_code_bit_or,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    PERCENTILE_APPROX(status_code, 0.9) AS status_code_p90
FROM flattened_attrs
GROUP BY CASE WHEN status_code >= 411 THEN 'HIGH' ELSE 'LOW' END
HAVING warn_count > 1
ORDER BY status_code_p90 NULLS FIRST;
