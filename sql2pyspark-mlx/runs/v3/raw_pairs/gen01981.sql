SELECT
    id,
    event_type,
    session_id,
    page
FROM web_events
WHERE page IS NULL OR page IN ('/search', '/product', '/cart')
ORDER BY session_id DESC, event_type
