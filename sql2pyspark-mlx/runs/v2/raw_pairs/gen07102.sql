SELECT s.id, s.started_at, s.country, t.min_event_time FROM sessions s INNER JOIN (SELECT session_id, MIN(event_time) min_event_time FROM web_events GROUP BY session_id) t ON s.id = t.session_id;
