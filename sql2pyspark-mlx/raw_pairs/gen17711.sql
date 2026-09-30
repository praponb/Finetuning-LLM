WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, host
    FROM exploded
    INTERSECT ALL
    SELECT id, host
    FROM app_logs
    WHERE level IN ('ERROR', 'INFO')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN matched_keys k ON s.id = k.id AND s.host = k.host
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v,
        SUM(latency_ms) OVER w_grp AS service_sum_latency_ms
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY service)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        key,
        v,
        service_sum_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY MONTH(log_ts), host ORDER BY latency_ms ASC NULLS FIRST) AS latency_ms_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY service, host ORDER BY latency_ms ASC) AS latency_ms_percent_rank_2
    FROM enriched
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN key = 'trace' THEN 1 ELSE 0 END AS is_trace,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM with_windows
    WHERE key IS NOT DISTINCT FROM 'region'
)
-- Aggregate with conditional metrics
SELECT
    key,
    CASE WHEN is_trace >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_trace_tier,
    COUNT_IF(key = 'version') AS version_events,
    PERCENTILE_APPROX(is_trace, ARRAY(0.25, 0.5, 0.75)) AS is_trace_quartiles,
    COUNT(DISTINCT key) AS distinct_keys,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    ROUND(SUM(latency_ms_percent_rank) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS latency_ms_percent_rank_per_trace,
    COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
    GROUPING_ID() AS grouping_level
FROM base
GROUP BY GROUPING SETS ((key, CASE WHEN is_trace >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END), (key), ())
ORDER BY is_trace_tier DESC
