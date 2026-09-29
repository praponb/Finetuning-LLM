WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        status_code,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        COALESCE(context['version'], 'direct') AS version_or_default,
        context['trace'] AS trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
    FROM app_logs
    WHERE status_code = 367
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    service,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    MAX(log_ts) AS last_seen,
    RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', log_ts) ORDER BY COUNT(CASE WHEN service = 'search' THEN 1 END) DESC) AS search_count_rank
FROM parsed
GROUP BY DATE_TRUNC('HOUR', log_ts), service
ORDER BY search_count DESC NULLS FIRST
