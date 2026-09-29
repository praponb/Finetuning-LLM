WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
)
-- Running sum of the flag = session number
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM flagged
