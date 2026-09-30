WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id, attr_key) AS dedup_rank
    FROM lines
    QUALIFY dedup_rank = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM latest
    INTERSECT
    SELECT level
    FROM app_logs
    WHERE service IN ('billing', 'search', 'auth')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.level = k.level
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        attr_key,
        v,
        dedup_rank,
        AVG(status_code) OVER (PARTITION BY TO_DATE(log_ts), attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_avg_1h,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        MAX(status_code) OVER (PARTITION BY level, attr_key) AS level_max_status_code
    FROM verified
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_avg_1h >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_avg_1h_tier,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT_IF(attr_key = 'region') AS region_events,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    GROUPING(CASE WHEN status_code_avg_1h >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM enriched
GROUP BY ROLLUP (CASE WHEN status_code_avg_1h >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING COUNT(DISTINCT attr_key) > 1
ORDER BY status_code_avg_1h_tier NULLS FIRST;
