WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(event_time) AS event_time_date,
        TRIM(page) AS clean_page,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time >= DATE '2023-08-28'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        event_time,
        event_time_date,
        clean_page,
        resolved_page,
        LAG(duration_sec, 1, 0) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_duration_sec,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        NTILE(4) OVER (PARTITION BY TO_DATE(event_time), page ORDER BY duration_sec, id) AS duration_sec_bucket_2,
        LEAD(duration_sec, 2, 0) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY event_time, id) AS next_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior
    FROM filtered
    WHERE event_time < '2025-08-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        event_time_date,
        clean_page,
        resolved_page,
        prev_duration_sec,
        duration_sec_bucket_2,
        next_duration_sec,
        duration_sec_avg_6_prior,
        SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_bucket_2 DESC, id) = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', event_time_date) AS activity_month,
    event_type,
    APPROX_PERCENTILE(session_no, 0.5) AS session_no_p50,
    ROUND(AVG(prev_duration_sec), 2) AS avg_prev_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
FROM latest
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', event_time_date), event_type), (DATE_TRUNC('MONTH', event_time_date)))
ORDER BY activity_month DESC;
