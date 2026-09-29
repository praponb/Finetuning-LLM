SELECT s.country, MAX(we.event_time) AS latest_event_time FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE we.session_id IN (5, 20) GROUP BY s.country;
