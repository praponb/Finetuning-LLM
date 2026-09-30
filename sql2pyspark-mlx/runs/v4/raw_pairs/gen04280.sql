WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        DENSE_RANK() OVER w_ordered AS latency_ms_dense_rank,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_max_2h,
        DENSE_RANK() OVER w_running AS status_code_dense_rank
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST), w_running AS (PARTITION BY service ORDER BY status_code DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_sum_1h,
        latency_ms_dense_rank,
        latency_ms_max_2h,
        status_code_dense_rank,
        DATE_TRUNC('year', log_ts) AS log_ts_period,
        ROUND(status_code_dense_rank * 1.07, 2) AS adjusted_status_code_dense_rank
    FROM scored
    WHERE k IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(status_code_sum_1h) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_sum_1h_max_30m,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS latency_ms_avg_7d,
        COUNT(adjusted_status_code_dense_rank) OVER (PARTITION BY service ORDER BY CAST(log_ts_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS adjusted_status_code_dense_rank_count_7d
    FROM cleaned
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_sum_1h,
        latency_ms_dense_rank,
        latency_ms_max_2h,
        status_code_dense_rank,
        log_ts_period,
        adjusted_status_code_dense_rank,
        status_code_sum_1h_max_30m,
        latency_ms_avg_7d,
        adjusted_status_code_dense_rank_count_7d,
        MIN(latency_ms) OVER (PARTITION BY k) AS k_min_latency_ms,
        MAX(status_code_dense_rank) OVER (PARTITION BY service, k) AS service_max_status_code_dense_rank
    FROM enriched
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_sum_1h,
        latency_ms_dense_rank,
        status_code_dense_rank,
        log_ts_period,
        adjusted_status_code_dense_rank,
        status_code_sum_1h_max_30m,
        k_min_latency_ms,
        INITCAP(TRIM(host)) AS host_title,
        COALESCE(k, 'UNKNOWN') AS resolved_k
    FROM scored_app_logs
    WHERE k IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_sum_1h,
        latency_ms_dense_rank,
        status_code_dense_rank,
        log_ts_period,
        adjusted_status_code_dense_rank,
        status_code_sum_1h_max_30m,
        k_min_latency_ms,
        host_title,
        resolved_k,
        RANK() OVER (PARTITION BY host ORDER BY latency_ms_dense_rank DESC NULLS LAST) AS latency_ms_dense_rank_rank,
        COUNT(latency_ms_dense_rank) OVER (PARTITION BY k, host ORDER BY CAST(log_ts_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_count_30d,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms_dense_rank DESC NULLS FIRST) AS latency_ms_dense_rank_rank_2,
        ROUND(k_min_latency_ms / SUM(k_min_latency_ms) OVER (PARTITION BY k), 4) AS k_min_latency_ms_share
    FROM filtered
),
summary AS (
    -- Grouped metrics
    SELECT
        k,
        COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
        SORT_ARRAY(COLLECT_SET(k)) AS k_list,
        SUM(CASE WHEN k = 'region' THEN status_code_sum_1h ELSE 0 END) AS region_status_code_sum_1h,
        SUM(CASE WHEN k = 'region' THEN latency_ms_dense_rank_rank ELSE 0 END) AS region_latency_ms_dense_rank_rank,
        PERCENTILE_APPROX(status_code_sum_1h, ARRAY(0.25, 0.5, 0.75)) AS status_code_sum_1h_quartiles
    FROM metrics
    GROUP BY ROLLUP (k)
    HAVING COUNT(*) FILTER (WHERE k = 'version') >= 1
)
SELECT
    k,
    version_rows,
    region_status_code_sum_1h,
    status_code_sum_1h_quartiles,
    CASE WHEN k RLIKE '^(trace|region)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS k_zone
FROM summary
ORDER BY region_status_code_sum_1h DESC
