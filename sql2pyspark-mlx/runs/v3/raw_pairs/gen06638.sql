SELECT page, event_type, MAX(duration_sec) AS max_duration_sec FROM web_events WHERE event_type NOT IN ('search', 'click') AND page = '/checkout' GROUP BY page, event_type ORDER BY max_duration_sec
