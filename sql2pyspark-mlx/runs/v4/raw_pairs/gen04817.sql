WITH parsed AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        context['version'] AS version,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
    WHERE log_ts < '2023-08-01'
)
SELECT
    tier,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    COUNT(*) AS event_count,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    SUM(CASE WHEN tier = 'free' THEN status_code ELSE 0 END) AS free_status_code,
    GROUPING(tier) AS grouping_id
FROM parsed
GROUP BY CUBE (tier);
