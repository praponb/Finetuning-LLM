WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
FROM flagged;
