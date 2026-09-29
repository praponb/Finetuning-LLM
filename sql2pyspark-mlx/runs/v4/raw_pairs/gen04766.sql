WITH flattened_attrs AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        context['region'] AS region,
        COALESCE(context['trace'], 'direct') AS trace_or_default
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2024-10-17'
)
SELECT
    *,
    CORR(status_code, latency_ms) OVER w_part AS corr_status_code_latency_ms,
    AVG(latency_ms) OVER (PARTITION BY tier ORDER BY latency_ms, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_avg_3,
    FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY tier, host ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
FROM flattened_attrs
WINDOW w_part AS (PARTITION BY tier);
