SELECT session_id, duration_sec, page
FROM web_events
WHERE page = '/checkout';
