WITH unpacked AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        COALESCE(context['version'], 'direct') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        COALESCE(context['trace'], 'unknown') AS trace_or_default
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 10 AND 16
)
SELECT
    tier,
    PERCENTILE_APPROX(status_code, 0.95) AS status_code_p95,
    MAX(log_ts) AS ended_at,
    COUNT(*) FILTER (WHERE tier = 'pro') AS pro_rows,
    GROUPING_ID() AS grouping_id
FROM unpacked
GROUP BY ROLLUP (tier)
ORDER BY status_code_p95 DESC NULLS FIRST;
