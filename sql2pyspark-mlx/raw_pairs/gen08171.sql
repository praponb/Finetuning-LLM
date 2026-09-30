WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
parsed AS (
    SELECT
        id,
        host,
        status_code,
        k,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code < 410
),
metrics AS (
    SELECT
        id,
        k,
        attr_value,
        status_code_high,
        AVG(has_fault) OVER w_ordered AS last7_avg_has_fault,
        COUNT(has_fault) OVER w_ordered AS last7_count_has_fault,
        MIN(has_fault) OVER w_ordered AS last7_min_has_fault,
        SUM(has_fault) OVER w_ordered AS last7_sum_has_fault,
        AVG(status_code) OVER (PARTITION BY k, host ORDER BY has_fault, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_code_avg_6
    FROM parsed
    WHERE has_fault = 0
    WINDOW w_ordered AS (PARTITION BY host ORDER BY has_fault, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_avg_6 NULLS LAST, id, k) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
cleaned AS (
    SELECT
        id,
        k,
        attr_value,
        last7_min_has_fault,
        last7_sum_has_fault,
        status_code_avg_6,
        GREATEST(last7_avg_has_fault, last7_min_has_fault) AS greatest_last7_avg_has_fault_last7_min_has_fault
    FROM deduped
    WHERE last7_min_has_fault BETWEEN 0.24 AND 0.62
),
grouped AS (
    SELECT
        CASE WHEN last7_min_has_fault >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_min_has_fault_tier,
        CASE WHEN greatest_last7_avg_has_fault_last7_min_has_fault >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_last7_avg_has_fault_last7_min_has_fault_tier,
        CASE WHEN status_code_avg_6 >= 473 THEN 'LARGE' ELSE 'SMALL' END AS status_code_avg_6_tier,
        APPROX_PERCENTILE(status_code_avg_6, ARRAY(0.25, 0.5, 0.75)) AS status_code_avg_6_quartiles,
        COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
        SUM(CASE WHEN k = 'version' THEN greatest_last7_avg_has_fault_last7_min_has_fault ELSE 0 END) AS version_greatest_last7_avg_has_fault_last7_min_has_fault,
        SUM(last7_min_has_fault) FILTER (WHERE k = 'trace') AS trace_total_last7_min_has_fault,
        GROUPING(CASE WHEN last7_min_has_fault >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
    FROM cleaned
    GROUP BY GROUPING SETS ((CASE WHEN last7_min_has_fault >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN greatest_last7_avg_has_fault_last7_min_has_fault >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_code_avg_6 >= 473 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN last7_min_has_fault >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN greatest_last7_avg_has_fault_last7_min_has_fault >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END))
    HAVING SUM(last7_min_has_fault) FILTER (WHERE k = 'trace') > 0
)
SELECT
    last7_min_has_fault_tier,
    greatest_last7_avg_has_fault_last7_min_has_fault_tier,
    status_code_avg_6_tier,
    version_count,
    version_greatest_last7_avg_has_fault_last7_min_has_fault,
    trace_total_last7_min_has_fault
FROM grouped
ORDER BY status_code_avg_6_tier NULLS FIRST;
