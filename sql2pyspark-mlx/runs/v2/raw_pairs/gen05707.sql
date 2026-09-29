SELECT
    id,
    page,
    event_time,
    duration_sec,
    duration_sec - LAG(duration_sec, 1, 0) OVER (PARTITION BY page ORDER BY event_time, id) AS duration_sec_change
FROM web_events
WHERE page IN ('/search', '/cart');
