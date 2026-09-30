SELECT
    id,
    session_id,
    page,
    event_time,
    RANK() OVER (PARTITION BY session_id, page ORDER BY event_time) AS rnk
FROM web_events
WHERE page IN ('/search', '/home');
