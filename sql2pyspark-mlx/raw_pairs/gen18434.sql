WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, level
    FROM lines
    EXCEPT ALL
    SELECT id, level
    FROM app_logs
    WHERE latency_ms > 2010
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN matched_keys k ON s.id = k.id AND s.level = k.level
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY service, level ORDER BY latency_ms NULLS FIRST) latency_ms_dense_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms DESC) latency_ms_cume_dist,
        CUME_DIST() OVER (ORDER BY latency_ms DESC) latency_ms_cume_dist_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) latency_ms_share
    FROM kept_rows
    WHERE level != 'DEBUG'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(latency_ms) OVER (PARTITION BY key) key_min_latency_ms,
        DENSE_RANK() OVER (PARTITION BY host, level ORDER BY latency_ms_cume_dist_2 DESC) latency_ms_cume_dist_2_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY latency_ms_cume_dist DESC) latency_ms_cume_dist_percent_rank
    FROM with_windows
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_ordered latency_ms_share_dense_rank,
        ROUND(latency_ms_dense_rank / SUM(latency_ms_dense_rank) OVER (PARTITION BY service), 4) latency_ms_dense_rank_share,
        COUNT(latency_ms_cume_dist_2_dense_rank) OVER w_hist host_count_latency_ms_cume_dist_2_dense_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY key ORDER BY latency_ms_share DESC NULLS LAST), w_hist AS (PARTITION BY host)
)
-- Multi-level aggregation
SELECT
    level,
    key,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    SUM(CASE WHEN service = 'billing' THEN host_count_latency_ms_cume_dist_2_dense_rank ELSE 0 END) AS billing_host_count_latency_ms_cume_dist_2_dense_rank,
    RANK() OVER (PARTITION BY level ORDER BY SUM(CASE WHEN service = 'billing' THEN host_count_latency_ms_cume_dist_2_dense_rank ELSE 0 END) DESC) AS billing_host_count_latency_ms_cume_dist_2_dense_rank_rank
FROM metrics
GROUP BY 1, 2
HAVING COUNT(CASE WHEN service = 'billing' THEN 1 END) > 1
ORDER BY level DESC NULLS FIRST
