WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        context['trace'] AS trace,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        COALESCE(context['version'], 'direct') AS version_or_default
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS retry_count_tier,
    COUNT(DISTINCT trace) AS distinct_traces,
    PERCENTILE_APPROX(retry_count, ARRAY(0.25, 0.5, 0.75)) AS retry_count_quartiles,
    MAX(log_ts) AS last_seen,
    RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', log_ts) ORDER BY COUNT(DISTINCT trace) DESC) AS distinct_traces_rank
FROM parsed
GROUP BY DATE_TRUNC('HOUR', log_ts), CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY retry_count_tier DESC;
