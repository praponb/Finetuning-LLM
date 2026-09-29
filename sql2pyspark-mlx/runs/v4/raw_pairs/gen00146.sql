WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        session_start_flag,
        session_no,
        session_no * session_start_flag AS session_no_times_session_start_flag,
        COALESCE(page, 'OTHER') AS page_clean
    FROM islands
    WHERE event_time > DATE '2023-12-10'
      AND page IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(session_start_flag) OVER (PARTITION BY event_type) AS event_type_count_session_start_flag,
        AVG(session_no) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_no_avg_30d,
        NTILE(10) OVER (ORDER BY session_no_times_session_start_flag, id) AS session_no_times_session_start_flag_bucket,
        COUNT(session_no) OVER (PARTITION BY event_type) AS event_type_count_session_no
    FROM cleaned
)
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    session_start_flag,
    session_no_times_session_start_flag,
    page_clean,
    event_type_count_session_start_flag
FROM scored
ORDER BY id DESC, duration_sec
LIMIT 10
