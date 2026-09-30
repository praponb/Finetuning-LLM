SELECT id, duration_sec
FROM web_events
WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events WHERE event_type IN ('page_view', 'search', 'add_to_cart'))
ORDER BY id DESC;
