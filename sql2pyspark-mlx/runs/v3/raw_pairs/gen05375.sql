SELECT event_time, page
FROM web_events
WHERE page = '/search'
ORDER BY 1
