WITH flattened AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
scored AS (
    SELECT
        id,
        has_fault,
        status_code_high,
        SUM(has_fault) OVER (PARTITION BY attr_key) AS attr_key_sum_has_fault,
        CUME_DIST() OVER (ORDER BY has_fault DESC NULLS FIRST) AS has_fault_cume_dist,
        RANK() OVER (PARTITION BY attr_key ORDER BY has_fault DESC NULLS FIRST) AS has_fault_rank
    FROM parsed
),
filtered AS (
    SELECT
        *,
        ROUND(has_fault_rank * 1.15, 2) AS has_fault_rank_adjusted,
        ROUND(attr_key_sum_has_fault * 0.9, 2) AS adjusted_attr_key_sum_has_fault
    FROM scored
    WHERE has_fault != 0
),
cleaned AS (
    SELECT
        *,
        adjusted_attr_key_sum_has_fault - status_code_high AS adjusted_attr_key_sum_has_fault_minus_status_code_high,
        LEAST(has_fault_rank, has_fault) AS least_has_fault_rank_has_fault
    FROM filtered
    WHERE has_fault_rank != 3
),
filtered_app_logs AS (
    SELECT
        id,
        has_fault,
        status_code_high,
        attr_key_sum_has_fault,
        has_fault_cume_dist,
        adjusted_attr_key_sum_has_fault,
        adjusted_attr_key_sum_has_fault_minus_status_code_high,
        least_has_fault_rank_has_fault,
        ROUND(least_has_fault_rank_has_fault / has_fault_rank, 2) AS least_has_fault_rank_has_fault_per_has_fault_rank,
        attr_key_sum_has_fault / NULLIF(has_fault_cume_dist, 0) AS attr_key_sum_has_fault_per_has_fault_cume_dist
    FROM cleaned
    WHERE adjusted_attr_key_sum_has_fault_minus_status_code_high >= 31
)
SELECT
    CASE WHEN adjusted_attr_key_sum_has_fault >= 85 THEN 'HIGH' ELSE 'LOW' END AS adjusted_attr_key_sum_has_fault_tier,
    CASE WHEN has_fault_cume_dist >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_cume_dist_tier,
    SUM(attr_key_sum_has_fault) AS total_attr_key_sum_has_fault,
    COUNT(*) AS cnt,
    ROUND(AVG(attr_key_sum_has_fault_per_has_fault_cume_dist), 2) AS avg_attr_key_sum_has_fault_per_has_fault_cume_dist,
    COUNT(*) AS event_count,
    ROUND(AVG(has_fault_cume_dist), 2) AS avg_has_fault_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS gid
FROM filtered_app_logs
GROUP BY GROUPING SETS ((CASE WHEN adjusted_attr_key_sum_has_fault >= 85 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN has_fault_cume_dist >= 0.67 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN adjusted_attr_key_sum_has_fault >= 85 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY avg_has_fault_cume_dist ASC NULLS LAST
