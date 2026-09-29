WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
    WHERE host IN ('h3', 'h2')
)
-- Multi-level aggregation
SELECT
    host,
    COUNT(*) FILTER (WHERE host = 'h3') AS h3_rows,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list
FROM parsed
GROUP BY ROLLUP (host);
