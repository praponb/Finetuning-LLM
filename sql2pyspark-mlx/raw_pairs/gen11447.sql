WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        page,
        event_time,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.burst_id,
    (SELECT SUM(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM islands s
ORDER BY page DESC NULLS LAST, sum_web_event_duration_sec
