WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM flattened
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        v,
        LAG(status_code) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS prev_status_code,
        LEAD(status_code) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS next_status_code,
        LEAD(latency_ms) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS next_latency_ms,
        LAG(latency_ms) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS prev_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY has_fault DESC, id, key) AS event_seq_2
    FROM parsed
    WHERE has_fault > 1
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        key,
        v,
        prev_status_code,
        next_status_code,
        prev_latency_ms,
        event_seq,
        event_seq_2,
        SUM(next_latency_ms) OVER (PARTITION BY key ORDER BY event_seq_2, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_latency_ms,
        MIN(next_latency_ms) OVER (PARTITION BY key ORDER BY event_seq_2, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_latency_ms,
        COUNT(next_latency_ms) OVER (PARTITION BY key ORDER BY event_seq_2, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY event_seq, id, key) AS row_seq
    FROM enriched
),
enriched_app_logs_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        v,
        prev_status_code,
        event_seq,
        running_min_next_latency_ms,
        row_seq,
        LAG(running_sum_next_latency_ms) OVER (PARTITION BY key ORDER BY next_status_code, id, key) AS prev_running_sum_next_latency_ms,
        LEAD(running_sum_next_latency_ms) OVER (PARTITION BY key ORDER BY next_status_code, id, key) AS next_running_sum_next_latency_ms,
        LEAD(event_seq) OVER (PARTITION BY key ORDER BY next_status_code, id, key) AS next_event_seq,
        LAG(event_seq) OVER (PARTITION BY key ORDER BY next_status_code, id, key) AS prev_event_seq,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY next_status_code, id, key) AS event_seq_3,
        LAST_VALUE(running_count_next_latency_ms, TRUE) OVER (PARTITION BY key ORDER BY running_count_next_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_count_next_latency_ms
    FROM enriched_app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_status_code >= 418 THEN 'HIGH' ELSE 'LOW' END AS prev_status_code_tier,
    key,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS gid
FROM enriched_app_logs_2
GROUP BY GROUPING SETS ((CASE WHEN prev_status_code >= 418 THEN 'HIGH' ELSE 'LOW' END, key), (CASE WHEN prev_status_code >= 418 THEN 'HIGH' ELSE 'LOW' END))
HAVING avg_row_seq < 1000000;
