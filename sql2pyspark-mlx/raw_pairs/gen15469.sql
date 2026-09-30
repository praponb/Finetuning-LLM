WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        latency_ms,
        status_code,
        key
    FROM lines
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id, key) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) latency_ms_share,
        AVG(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) status_code_avg_5,
        NTILE(4) OVER (PARTITION BY key ORDER BY latency_ms, id, key) latency_ms_bucket
    FROM latest
    WHERE key = 'version'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        latency_ms_share,
        latency_ms_bucket,
        AVG(latency_ms_bucket) OVER (PARTITION BY key ORDER BY status_code_avg_5, id, key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) latency_ms_bucket_avg_3,
        MIN(status_code) OVER (PARTITION BY key) key_min_status_code,
        MAX(latency_ms_bucket) OVER (PARTITION BY key) key_max_latency_ms_bucket
    FROM metrics
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        key,
        latency_ms_bucket_avg_3,
        key_min_status_code,
        latency_ms_bucket * key_max_latency_ms_bucket latency_ms_bucket_times_key_max_latency_ms_bucket
    FROM enriched
    WHERE key_max_latency_ms_bucket = 4
),
latest_app_logs AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY key ORDER BY latency_ms_bucket_avg_3 DESC) = 1
),
latest_app_logs_2 AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        key,
        latency_ms_bucket_avg_3,
        key_min_status_code,
        latency_ms_bucket_times_key_max_latency_ms_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY key_min_status_code DESC NULLS LAST, id, key) AS rank_in_group
        FROM latest_app_logs
    ) x
    WHERE rank_in_group = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN key_min_status_code >= 440 THEN 'LARGE' ELSE 'SMALL' END AS key_min_status_code_tier,
        COUNT_IF(key = 'trace') AS trace_events,
        APPROX_PERCENTILE(latency_ms_bucket_avg_3, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_bucket_avg_3_quartiles,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        PERCENTILE_APPROX(key_min_status_code, 0.5) AS key_min_status_code_p50
    FROM latest_app_logs_2
    GROUP BY key_min_status_code_tier
)
SELECT key_min_status_code_tier, trace_events
FROM summary
