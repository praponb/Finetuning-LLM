WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        log_ts,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        context['version'] AS version,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        COALESCE(context['version'], 'unknown') AS version_or_default
    FROM app_logs
    WHERE log_ts >= '2024-08-10'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    host,
    MIN(log_ts) AS started_at,
    MIN(log_ts) AS started_at_2,
    COUNT(*) AS event_count
FROM parsed
GROUP BY DATE_TRUNC('DAY', log_ts), host
ORDER BY host NULLS LAST;
