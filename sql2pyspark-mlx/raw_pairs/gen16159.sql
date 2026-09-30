WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_code) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_status_code,
        AVG(status_code) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_code,
        MIN(status_code) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_code,
        SUM(status_code) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_status_code,
        COUNT(latency_ms) OVER (PARTITION BY MONTH(log_ts), host) AS log_ts_month_count_latency_ms
    FROM lines
    WHERE latency_ms = 2247
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY running_avg_status_code ASC NULLS FIRST, id ASC, attr_key ASC) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        value,
        running_count_status_code,
        running_avg_status_code,
        running_min_status_code,
        running_sum_status_code,
        rn,
        MAX(status_code) OVER (PARTITION BY attr_key ORDER BY log_ts_month_count_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_code,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY log_ts_month_count_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_code_2,
        MIN(status_code) OVER (PARTITION BY attr_key ORDER BY log_ts_month_count_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_code_2,
        COUNT(running_sum_status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS running_sum_status_code_count_2h
    FROM ranked
)
-- Multi-level aggregation
SELECT
    service,
    APPROX_PERCENTILE(latency_ms, 0.5) AS latency_ms_p50,
    COUNT(*) FILTER (WHERE service = 'billing') AS billing_rows,
    APPROX_PERCENTILE(running_count_status_code, ARRAY(0.25, 0.5, 0.75)) AS running_count_status_code_quartiles,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT_IF(service = 'billing') AS billing_events,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY service
ORDER BY distinct_ids DESC;
