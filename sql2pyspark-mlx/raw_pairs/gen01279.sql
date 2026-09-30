SELECT s.started_at, we.duration_sec, HOUR(we.event_time) AS event_time_hour
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE s.country != 'US' AND we.session_id = 6;
