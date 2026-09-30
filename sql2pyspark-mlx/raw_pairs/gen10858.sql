WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    session_start_flag,
    session_no
FROM sessionized
ORDER BY session_start_flag DESC NULLS FIRST;
