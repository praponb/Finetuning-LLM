WITH unpacked AS (
    SELECT
        id,
        latency_ms,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        COALESCE(context['trace'], 'direct') AS trace_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
)
SELECT
    CASE WHEN latency_ms >= 2502 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    tier,
    SUM(CASE WHEN tier = 'gold' THEN latency_ms ELSE 0 END) AS gold_latency_ms,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95,
    AVG(latency_ms) FILTER (WHERE tier = 'pro') AS pro_avg_latency_ms,
    COUNT_IF(tier = 'free') AS free_events,
    SIZE(COLLECT_SET(tier)) AS num_tiers,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95_2
FROM unpacked
GROUP BY CASE WHEN latency_ms >= 2502 THEN 'LARGE' ELSE 'SMALL' END, tier
ORDER BY num_tiers;
