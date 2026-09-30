WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.is_new_burst,
    s.burst_id,
    (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS max_web_event_duration_sec
FROM islands s
