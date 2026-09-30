WITH flattened AS (
    SELECT
        id,
        service,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
base AS (
    SELECT
        *,
        LOWER(service) AS service_lower,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM flattened
    WHERE attr_key NOT IN ('version', 'region')
),
filtered AS (
    SELECT
        id,
        attr_key,
        attr_value,
        service_lower,
        resolved_attr_key,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END AS is_trace
    FROM base
    WHERE attr_key IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY is_trace DESC NULLS FIRST) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
base_app_logs AS (
    SELECT
        id,
        attr_key,
        attr_value,
        service_lower,
        resolved_attr_key,
        dedup_rank / NULLIF(is_trace, 0) AS dedup_rank_per_is_trace,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM latest
    WHERE is_trace BETWEEN 54 AND 80
),
filtered_app_logs AS (
    SELECT
        id,
        attr_key,
        attr_value,
        resolved_attr_key,
        dedup_rank_per_is_trace,
        attr_key_filled,
        CEIL(dedup_rank_per_is_trace) AS dedup_rank_per_is_trace_whole
    FROM base_app_logs
    WHERE attr_key IS NOT NULL
)
SELECT
    CASE WHEN dedup_rank_per_is_trace >= 60 THEN 'HIGH' ELSE 'LOW' END AS dedup_rank_per_is_trace_tier,
    CASE WHEN dedup_rank_per_is_trace_whole >= 83 THEN 'LARGE' ELSE 'SMALL' END AS dedup_rank_per_is_trace_whole_tier,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    APPROX_PERCENTILE(dedup_rank_per_is_trace, ARRAY(0.25, 0.5, 0.75)) AS dedup_rank_per_is_trace_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN attr_key = 'region' THEN dedup_rank_per_is_trace_whole ELSE 0 END) AS region_dedup_rank_per_is_trace_whole,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count
FROM filtered_app_logs
GROUP BY ROLLUP (CASE WHEN dedup_rank_per_is_trace >= 60 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN dedup_rank_per_is_trace_whole >= 83 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY dedup_rank_per_is_trace_whole_tier DESC NULLS LAST;
