SELECT
    session_id,
    SUM(CASE WHEN page = '/product' THEN duration_sec ELSE 0 END) AS product_duration_sec,
    SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec,
    SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec
FROM web_events
WHERE duration_sec > 166
GROUP BY session_id
HAVING SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) > 79
