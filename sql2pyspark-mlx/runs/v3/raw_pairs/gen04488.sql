SELECT *, CASE WHEN page IN ('/search', '/product') THEN 'Group A' ELSE 'Other' END AS page_group, CONCAT_WS('-', page, event_type) AS page_event_type_key
FROM web_events
WHERE duration_sec = 216
ORDER BY duration_sec, event_time DESC;
