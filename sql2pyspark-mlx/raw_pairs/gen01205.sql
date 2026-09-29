WITH flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM flagged;
