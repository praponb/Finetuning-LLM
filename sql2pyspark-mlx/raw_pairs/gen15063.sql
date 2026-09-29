WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(latency_ms) OVER w_running AS running_min_latency_ms,
        AVG(latency_ms) OVER w_running AS running_avg_latency_ms,
        SUM(latency_ms) OVER w_running AS running_sum_latency_ms,
        MAX(latency_ms) OVER w_running AS running_max_latency_ms,
        LEAD(latency_ms, 1) OVER w_running_2 AS next_latency_ms
    FROM flattened
    WHERE level = 'WARN'
    WINDOW w_running AS (PARTITION BY level ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (ORDER BY latency_ms, id, k)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(latency_ms, 0) AS latency_ms_rounded,
        COALESCE(k, 'OTHER') AS k_clean
    FROM scored
    WHERE k IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        k,
        running_max_latency_ms,
        latency_ms_rounded,
        k_clean,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY running_avg_latency_ms DESC NULLS LAST) AS running_avg_latency_ms_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY running_sum_latency_ms, id, k) AS event_seq,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY running_sum_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms
    FROM filtered
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY running_max_latency_ms DESC NULLS FIRST) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    latency_ms,
    k,
    running_max_latency_ms,
    latency_ms_rounded,
    event_seq,
    last_known_latency_ms,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'CORE' WHEN k RLIKE '^(region)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS k_zone
FROM deduped
