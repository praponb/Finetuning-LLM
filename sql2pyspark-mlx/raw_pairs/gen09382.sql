SELECT id, duration_sec FROM web_events WHERE HOUR(event_time) BETWEEN 7 AND 10 INTERSECT SELECT id, duration_sec FROM web_events WHERE event_type IN ('click', 'search', 'page_view');
