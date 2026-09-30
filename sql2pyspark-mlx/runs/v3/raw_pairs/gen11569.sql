SELECT
    id,
    event_type,
    session_id,
    CASE WHEN page IN ('/cart', '/product') THEN 'Priority' ELSE 'Other' END AS page_group
FROM web_events
ORDER BY 1 DESC
