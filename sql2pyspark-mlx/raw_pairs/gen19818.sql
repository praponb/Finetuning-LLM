WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY latency_ms DESC NULLS FIRST, id ASC, attr_key ASC) AS latest_rank
    FROM exploded
    QUALIFY latest_rank = 1
),
latest_app_logs AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST, id ASC, attr_key ASC) AS latest_rank_2
    FROM latest
    QUALIFY latest_rank_2 = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, service ORDER BY latest_rank_2 ASC, id ASC, attr_key ASC) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(latency_ms) OVER w_running running_count_latency_ms,
        AVG(latency_ms) OVER w_running running_avg_latency_ms,
        SUM(latency_ms) OVER w_running running_sum_latency_ms,
        SUM(latency_ms) OVER (ORDER BY latest_rank ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_latency_ms
    FROM ranked
    WINDOW w_running AS (PARTITION BY level ORDER BY latest_rank ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY running_count_latency_ms ASC NULLS FIRST, id ASC, attr_key ASC) AS rn
    FROM metrics
    QUALIFY rn = 1
)
-- Grouped metrics
SELECT
    CASE WHEN running_latency_ms >= 10536 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_latency_ms_tier,
    CASE WHEN running_avg_latency_ms >= 2630 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_avg_latency_ms_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM deduped
GROUP BY running_latency_ms_tier, running_avg_latency_ms_tier
ORDER BY running_avg_latency_ms_tier ASC NULLS FIRST
