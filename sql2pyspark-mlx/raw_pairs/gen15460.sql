WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        payload_json,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        COALESCE(context['trace'], 'direct') AS trace_or_default
    FROM app_logs
    WHERE log_ts >= '2024-08-01'
)
-- Multi-level aggregation
SELECT
    host,
    CASE WHEN latency_ms >= 1870 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    tier,
    COUNT(CASE WHEN tier = 'free' THEN 1 END) AS free_count,
    COUNT(*) AS event_count,
    MAX(latency_ms) AS max_latency_ms
FROM unpacked
GROUP BY host, latency_ms_tier, tier
HAVING COUNT(*) < 1000
ORDER BY tier;
