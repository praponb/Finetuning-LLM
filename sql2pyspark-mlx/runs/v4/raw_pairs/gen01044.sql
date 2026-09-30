SELECT
    id,
    event_type,
    page,
    event_time,
    COUNT(*) OVER (PARTITION BY event_type) AS event_type_count,
    RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rnk
FROM web_events
WHERE page IN ('/checkout', '/product');
