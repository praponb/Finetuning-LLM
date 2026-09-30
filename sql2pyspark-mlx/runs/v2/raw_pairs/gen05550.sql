SELECT s.device, MAX(we.event_time) AS latest_event_time FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE we.page != '/home' GROUP BY s.device ORDER BY latest_event_time
