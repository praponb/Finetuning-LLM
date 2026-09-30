WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        level,
        latency_ms,
        context,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
-- Ranking and gap features
SELECT
    id,
    latency_ms,
    user_tier,
    FIRST_VALUE(latency_ms, TRUE) OVER (PARTITION BY host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
    FIRST_VALUE(user_tier, TRUE) OVER (PARTITION BY level, host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_user_tier
FROM unpacked
WHERE latency_ms BETWEEN 1840 AND 1886
