WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        host,
        log_ts,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status_code_high >= 2 THEN 'High' WHEN status_code_high >= 1 THEN 'Medium' ELSE 'Low' END AS status_code_high_band
    FROM parsed
    WHERE log_ts > TIMESTAMP '2025-06-01 13:00:00'
      AND attr_key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_code_high) OVER (PARTITION BY attr_key) AS attr_key_count_status_code_high,
        SUM(has_fault) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS has_fault_sum_2h,
        SUM(status_code_high) OVER (PARTITION BY host) AS host_sum_status_code_high,
        PERCENT_RANK() OVER (ORDER BY has_fault NULLS FIRST) AS has_fault_percent_rank,
        AVG(status_code_high) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_high_avg_30m
    FROM filtered
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY has_fault DESC NULLS LAST) AS has_fault_dense_rank,
        DENSE_RANK() OVER (PARTITION BY host, attr_key ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_dense_rank,
        RANK() OVER (PARTITION BY host, attr_key ORDER BY status_code_high_avg_30m DESC NULLS FIRST) AS status_code_high_avg_30m_rank
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(status_code_high_dense_rank / SUM(status_code_high_dense_rank) OVER (PARTITION BY attr_key), 4) AS status_code_high_dense_rank_share,
        COUNT(has_fault_sum_2h) OVER w_recent AS host_count_has_fault_sum_2h,
        ROUND(status_code_high_avg_30m_rank / SUM(status_code_high_avg_30m_rank) OVER (PARTITION BY host, attr_key), 4) AS status_code_high_avg_30m_rank_share,
        MAX(has_fault_dense_rank) OVER w_recent_2 AS has_fault_dense_rank_max_15m,
        STDDEV_SAMP(has_fault_percent_rank) OVER w_recent AS host_stddev_samp_has_fault_percent_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY host), w_recent_2 AS (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    attr_key,
    COUNT_IF(host = 'h3') AS h3_events,
    APPROX_PERCENTILE(has_fault_sum_2h, ARRAY(0.25, 0.5, 0.75)) AS has_fault_sum_2h_quartiles,
    COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    MIN(log_ts) AS started_at,
    COUNT_IF(attr_key = 'region') AS region_events
FROM with_windows
GROUP BY attr_key
