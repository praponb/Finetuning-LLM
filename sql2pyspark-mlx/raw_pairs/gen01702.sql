WITH flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
)
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM flagged
