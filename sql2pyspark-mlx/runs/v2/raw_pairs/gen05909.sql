SELECT event_type, page
FROM web_events
WHERE event_type <> 'page_view'
UNION ALL
SELECT event_type, page
FROM web_events
WHERE event_time >= '2024-03-01'
ORDER BY page
