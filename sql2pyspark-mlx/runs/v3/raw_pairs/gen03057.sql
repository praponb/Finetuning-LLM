SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    session_id,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
    DENSE_RANK() OVER (PARTITION BY session_id ORDER BY event_time) AS dense_rnk
FROM web_events
WHERE page IN ('/cart', '/checkout', '/home');
