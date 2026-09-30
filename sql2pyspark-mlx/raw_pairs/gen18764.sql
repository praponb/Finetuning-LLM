WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
filtered AS (
    SELECT *, LENGTH(host) AS host_length
    FROM flattened
    WHERE log_ts > TO_DATE('2025-06-01')
      AND key IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MIN(host_length) OVER (PARTITION BY key) AS key_min_host_length,
        PERCENT_RANK() OVER (PARTITION BY MONTH(log_ts) ORDER BY host_length ASC NULLS FIRST) AS host_length_percent_rank,
        MAX(host_length) OVER (PARTITION BY host) AS host_max_host_length,
        CUME_DIST() OVER (PARTITION BY YEAR(log_ts), host ORDER BY host_length ASC NULLS FIRST) AS host_length_cume_dist
    FROM filtered
),
base AS (
    SELECT
        *,
        host_length_cume_dist * key_min_host_length AS host_length_cume_dist_times_key_min_host_length,
        HOUR(log_ts) AS log_ts_hour,
        COALESCE(key, 'NONE') AS key_filled
    FROM metrics
    WHERE key IS NOT NULL
      AND log_ts > TIMESTAMP '2025-06-01 08:00:00'
)
SELECT
    service,
    MAX(log_ts) AS last_log_ts,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    SUM(host_length_percent_rank) AS total_host_length_percent_rank
FROM base
GROUP BY service
ORDER BY h1_rows ASC
