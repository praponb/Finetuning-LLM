WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM flagged;
