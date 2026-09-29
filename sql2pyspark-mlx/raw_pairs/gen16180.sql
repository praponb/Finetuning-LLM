WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        level,
        status_code,
        payload_json,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        context['region'] AS region,
        context['version'] AS version
    FROM app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN status_code >= 472 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    SUM(CASE WHEN level = 'INFO' THEN is_flagged ELSE 0 END) AS info_is_flagged,
    COUNT_IF(host = 'h2') AS h2_events,
    SIZE(COLLECT_SET(host)) AS num_hosts
FROM parsed
GROUP BY CASE WHEN status_code >= 472 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT_IF(host = 'h2') < 1000
