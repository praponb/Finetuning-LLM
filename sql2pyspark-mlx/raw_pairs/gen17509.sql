WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(log_ts) AS log_ts_weekofyear,
        CASE WHEN service = 'search' THEN 1 ELSE 0 END AS is_search
    FROM flattened
    WHERE key IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_search) OVER (PARTITION BY MONTH(log_ts) ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_search_max_1h,
        COUNT(is_search) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_search_count_1h,
        COUNT(is_search) OVER (PARTITION BY key) AS key_count_is_search
    FROM valid
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN service = 'auth' THEN 1 ELSE 0 END AS is_auth
    FROM enriched
    WHERE key IS NOT NULL
      AND status_code = 438
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(is_search_count_1h) OVER w_running AS is_search_count_1h_max_2h,
        ROUND(is_search_count_1h / SUM(is_search_count_1h) OVER (PARTITION BY key), 4) AS is_search_count_1h_share,
        MAX(is_search) OVER (PARTITION BY key) AS key_max_is_search,
        SUM(log_ts_weekofyear) OVER w_recent AS log_ts_weekofyear_sum_15m,
        SUM(is_search_count_1h) OVER (PARTITION BY key, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_search_count_1h_sum_2h
    FROM filtered
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY TO_DATE(log_ts) ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    service,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SIZE(COLLECT_SET(service)) AS num_services,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT key) AS distinct_keys,
    MAX(is_search_count_1h_share) FILTER (WHERE key = 'trace') AS trace_max_is_search_count_1h_share
FROM with_windows
GROUP BY GROUPING SETS ((service, DATE_TRUNC('HOUR', log_ts)), (service))
HAVING trace_max_is_search_count_1h_share < 1000000
ORDER BY distinct_ids;
