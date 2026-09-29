WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        value,
        LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY attr_key ORDER BY status_code ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms,
        LEAD(latency_ms, 1) OVER (PARTITION BY attr_key ORDER BY status_code ASC, id ASC, attr_key ASC) AS next_latency_ms,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY attr_key ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key
    FROM lines
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        value,
        last_known_latency_ms,
        next_latency_ms,
        first_known_attr_key,
        MAX(next_latency_ms) OVER w_part AS last7_max_next_latency_ms,
        COUNT(next_latency_ms) OVER w_part AS last7_count_next_latency_ms,
        AVG(next_latency_ms) OVER w_part AS last7_avg_next_latency_ms,
        ROUND(next_latency_ms / SUM(next_latency_ms) OVER (PARTITION BY attr_key), 4) AS next_latency_ms_share
    FROM enriched
    WINDOW w_part AS (PARTITION BY attr_key ORDER BY next_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched_app_logs_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(last7_count_next_latency_ms, 1) OVER (PARTITION BY attr_key ORDER BY last7_count_next_latency_ms ASC, id ASC, attr_key ASC) AS next_last7_count_next_latency_ms,
        LAST_VALUE(last7_max_next_latency_ms, TRUE) OVER w_ordered AS last_known_last7_max_next_latency_ms,
        PERCENT_RANK() OVER w_hist AS last7_avg_next_latency_ms_percent_rank,
        COUNT(last7_max_next_latency_ms) OVER w_running AS last7_max_next_latency_ms_count_1h
    FROM enriched_app_logs
    WINDOW w_ordered AS (ORDER BY last_known_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY DATE_TRUNC('YEAR', log_ts), attr_key ORDER BY last7_avg_next_latency_ms DESC), w_running AS (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        attr_key,
        last_known_latency_ms,
        next_latency_ms,
        first_known_attr_key,
        last7_max_next_latency_ms,
        last7_avg_next_latency_ms,
        next_latency_ms_share,
        next_last7_count_next_latency_ms,
        last7_avg_next_latency_ms_percent_rank,
        last7_max_next_latency_ms_count_1h,
        GREATEST(last_known_latency_ms, last7_max_next_latency_ms_count_1h) AS greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h
    FROM enriched_app_logs_2
    WHERE next_latency_ms_share BETWEEN 0.51 AND 0.71
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        attr_key,
        next_latency_ms,
        first_known_attr_key,
        last7_max_next_latency_ms,
        last7_avg_next_latency_ms,
        last7_avg_next_latency_ms_percent_rank,
        greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h,
        INITCAP(TRIM(attr_key)) AS attr_key_title
    FROM cleaned
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'trace'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    log_ts,
    attr_key,
    first_known_attr_key,
    last7_max_next_latency_ms,
    greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h,
    new_group_flag,
    session_no
FROM islands
ORDER BY greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h ASC NULLS LAST;
