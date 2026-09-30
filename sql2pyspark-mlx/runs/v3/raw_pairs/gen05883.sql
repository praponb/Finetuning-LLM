SELECT event_time, session_id, DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label
FROM web_events
WHERE NOT (page NOT IN ('/search', '/home', '/checkout') AND duration_sec != 277)
