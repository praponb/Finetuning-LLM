WITH flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        COALESCE(context['region'], 'direct') AS region_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY YEAR(log_ts), host ORDER BY latency_ms ASC NULLS LAST) AS latency_ms_percent_rank,
    AVG(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2,
    LAG(duration_sec, 1) OVER (PARTITION BY service ORDER BY log_ts ASC, id ASC) AS prev_duration_sec,
    SUM(latency_ms) OVER (PARTITION BY MONTH(log_ts), level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_sum_30m
FROM flattened_attrs;
